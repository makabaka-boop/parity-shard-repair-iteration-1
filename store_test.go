package shardstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newTestStore(t *testing.T, opts ...Option) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := New(dir, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func mustPut(t *testing.T, s *Store, key string, data []byte, expectedGen int64) int64 {
	t.Helper()
	gen, err := s.Put(context.Background(), key, data, expectedGen)
	if err != nil {
		t.Fatalf("Put key=%q gen=%d: %v", key, expectedGen, err)
	}
	return gen
}

func mustGet(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	data, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get key=%q: %v", key, err)
	}
	return data
}

func shardFile(t *testing.T, s *Store, role int, key string, gen int64) string {
	t.Helper()
	return s.shardPath(role, key, gen)
}

// deleteShard removes one shard from disk (simulating a failed disk volume).
func deleteShard(t *testing.T, s *Store, role int, key string, gen int64) {
	t.Helper()
	if err := os.Remove(s.shardPath(role, key, gen)); err != nil {
		t.Fatalf("remove shard role=%d: %v", role, err)
	}
}

// corruptShard flips one byte of one shard in place (bit rot), keeping length.
func corruptShard(t *testing.T, s *Store, role int, key string, gen int64) {
	t.Helper()
	p := s.shardPath(role, key, gen)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read shard for corruption: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("cannot corrupt zero-length shard role=%d", role)
	}
	raw[0] ^= 0xFF
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatalf("write corrupt shard: %v", err)
	}
}

// listRegular returns regular files (recursive, relative paths) under root.
func listRegular(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func countDebris(t *testing.T, root string) (dotFiles int) {
	t.Helper()
	for _, rel := range listRegular(t, root) {
		base := filepath.Base(rel)
		if strings.HasPrefix(base, ".") {
			dotFiles++
		}
	}
	return dotFiles
}

func pollUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// ---------------------------------------------------------------------------
// layout: two equal data shards + XOR parity
// ---------------------------------------------------------------------------

func TestPutGetRoundTrip(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	cases := map[string][]byte{
		"empty":    []byte(""),
		"one byte": []byte("X"),
		"even":     bytes.Repeat([]byte("ab"), 100),
		"odd":      []byte("the quick brown fox jumps"),
		"unicode":  []byte("你好，世界🌍 — shard store"),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			key := "obj-" + name
			gen := mustPut(t, s, key, data, AnyGen)
			if gen != 1 {
				t.Fatalf("first gen = %d, want 1", gen)
			}
			if got := mustGet(t, s, key); !bytes.Equal(got, data) {
				t.Fatalf("data mismatch: got %q want %q", got, data)
			}

			m, err := s.Manifest(key)
			if err != nil {
				t.Fatalf("Manifest: %v", err)
			}
			if m.Length != len(data) {
				t.Fatalf("manifest length = %d, want %d", m.Length, len(data))
			}

			// The three shards must be equal length on disk.
			var sizes [3]int64
			var raws [3][]byte
			for role := 0; role < numShards; role++ {
				fi, err := os.Stat(shardFile(t, s, role, key, gen))
				if err != nil {
					t.Fatalf("stat shard %d: %v", role, err)
				}
				sizes[role] = fi.Size()
				raws[role], _ = os.ReadFile(shardFile(t, s, role, key, gen))
			}
			if sizes[0] != sizes[1] || sizes[1] != sizes[2] {
				t.Fatalf("shard lengths not equal: %v", sizes)
			}
			if want := int64((len(data) + 1) / 2); sizes[0] != want {
				t.Fatalf("shard length = %d, want %d", sizes[0], want)
			}
			// P = A xor B, byte-wise.
			if !bytes.Equal(raws[ShardParity], xorBytes(raws[ShardA], raws[ShardB])) {
				t.Fatal("parity shard != A xor B")
			}
		})
	}

	// Two objects do not interfere.
	mustPut(t, s, "k1", []byte("one"), AnyGen)
	mustPut(t, s, "k2", []byte("two"), AnyGen)
	if string(mustGet(t, s, "k1")) != "one" || string(mustGet(t, s, "k2")) != "two" {
		t.Fatal("objects interfere")
	}
	if _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key err = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// conditional generation update
// ---------------------------------------------------------------------------

func TestConditionalGenerationUpdate(t *testing.T) {
	s, dir := newTestStore(t)
	ctx := context.Background()
	key := "doc"

	if _, err := s.Put(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatalf("create with expectedGen 0: %v", err)
	}
	// Stale precondition before the object exists.
	if _, err := s.Put(ctx, key, []byte("v?"), 0); !errors.Is(err, ErrGenConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	gen := mustPut(t, s, key, []byte("v2"), 1)
	if gen != 2 {
		t.Fatalf("gen = %d, want 2", gen)
	}
	// Lost update: someone else already published gen 2.
	if _, err := s.Put(ctx, key, []byte("v2-again"), 1); !errors.Is(err, ErrGenConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if string(mustGet(t, s, key)) != "v2" {
		t.Fatal("conflicting write must not change published data")
	}
	mustPut(t, s, key, []byte("v3"), 2)
	if string(mustGet(t, s, key)) != "v3" {
		t.Fatal("conditional update did not publish v3")
	}
	if m, _ := s.Manifest(key); m.Gen != 3 {
		t.Fatalf("manifest gen = %d, want 3", m.Gen)
	}
	// Old generations must have been reaped; no temp debris either.
	for _, rel := range listRegular(t, dir) {
		base := filepath.Base(rel)
		if strings.HasPrefix(base, ".") {
			t.Fatalf("temp debris left behind: %s", rel)
		}
		if strings.Contains(base, ".g1") || strings.Contains(base, ".g2") {
			t.Fatalf("old generation shard left behind: %s", rel)
		}
	}
}

func TestConcurrentConditionalPutsSingleWinner(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "hot"
	mustPut(t, s, key, []byte("initial"), AnyGen) // gen 1

	const n = 16
	var wg sync.WaitGroup
	winners := make(chan int, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := s.Put(ctx, key, []byte(fmt.Sprintf("writer-%02d", i)), 1)
			if err == nil {
				winners <- i
			} else if !errors.Is(err, ErrGenConflict) {
				t.Errorf("writer %d unexpected err: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(winners)

	count := 0
	winner := -1
	for w := range winners {
		count++
		winner = w
	}
	if count != 1 {
		t.Fatalf("winners = %d, want exactly 1", count)
	}
	if got := string(mustGet(t, s, key)); got != fmt.Sprintf("writer-%02d", winner) {
		t.Fatalf("published data = %q, want winner writer-%02d", got, winner)
	}
}

// ---------------------------------------------------------------------------
// single-shard loss/corruption: rebuild and heal
// ---------------------------------------------------------------------------

func TestSingleShardFailureRebuilds(t *testing.T) {
	for _, badRole := range []int{ShardA, ShardB, ShardParity} {
		t.Run([]string{"missingA", "missingB", "missingParity"}[badRole], func(t *testing.T) {
			s, _ := newTestStore(t)
			key := "x"
			data := []byte("0123456789-abcdef")
			gen := mustPut(t, s, key, data, AnyGen)
			deleteShard(t, s, badRole, key, gen)

			healed, err := s.Repair(context.Background(), key)
			if err != nil {
				t.Fatalf("Repair: %v", err)
			}
			if !healed {
				t.Fatal("Repair reported no healing despite a missing shard")
			}
			if got := mustGet(t, s, key); !bytes.Equal(got, data) {
				t.Fatalf("data after repair = %q", got)
			}
			// Second repair is a no-op.
			healed, err = s.Repair(context.Background(), key)
			if err != nil || healed {
				t.Fatalf("idempotent repair: healed=%v err=%v", healed, err)
			}
		})

		t.Run([]string{"corruptA", "corruptB", "corruptParity"}[badRole], func(t *testing.T) {
			s, _ := newTestStore(t)
			key := "y"
			data := bytes.Repeat([]byte{0xAB, 0xCD}, 32)
			gen := mustPut(t, s, key, data, AnyGen)
			corruptShard(t, s, badRole, key, gen)

			// Get itself must rebuild transparently and heal on disk.
			if got := mustGet(t, s, key); !bytes.Equal(got, data) {
				t.Fatalf("Get after corruption = %x...", got[:8])
			}
			m, _ := s.Manifest(key)
			got, ok := readVerified(s.shardPath(badRole, key, m.Gen), m.Shards[badRole].SHA256)
			if !ok {
				t.Fatal("healed shard still fails verification on disk")
			}
			if len(got) == 0 {
				t.Fatal("healed shard empty")
			}
		})
	}
}

func TestEmptyObjectRepair(t *testing.T) {
	s, _ := newTestStore(t)
	key := "empty"
	gen := mustPut(t, s, key, []byte{}, AnyGen)
	deleteShard(t, s, ShardParity, key, gen)
	if healed, err := s.Repair(context.Background(), key); err != nil || !healed {
		t.Fatalf("repair empty object: healed=%v err=%v", healed, err)
	}
	if got := mustGet(t, s, key); len(got) != 0 {
		t.Fatalf("empty object after repair has len %d", len(got))
	}
}

// Odd-length objects leave a trailing zero pad in B and parity; rebuilding each
// role in turn must still reproduce the exact original bytes.
func TestOddLengthRepairEveryRole(t *testing.T) {
	data := []byte("0123456789ABCDEF?") // 18 bytes -> shards of 9 with B padded
	if len(data)%2 == 0 {
		t.Fatal("test payload must be odd length")
	}
	for _, badRole := range []int{ShardA, ShardB, ShardParity} {
		t.Run([]string{"A", "B", "P"}[badRole], func(t *testing.T) {
			s, _ := newTestStore(t)
			key := "odd"
			gen := mustPut(t, s, key, data, AnyGen)
			deleteShard(t, s, badRole, key, gen)
			if got := mustGet(t, s, key); !bytes.Equal(got, data) {
				t.Fatalf("rebuilt odd data = %q want %q", got, data)
			}
		})
	}
}

// Simulate a crash in the middle of the three shard renames: one gen-2 shard
// already has its final name, another is still a temp file, gen 1 is published.
// Startup recovery must leave exactly the published gen-1 layout.
func TestCrashMidwayThroughShardRenames(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := "doc"
	v1 := []byte("published-one")
	gen1 := mustPut(t, s1, key, v1, AnyGen)
	s1.Close()

	// Build gen-2 debris directly on disk: one renamed shard, one temp shard,
	// and no gen-2 manifest.
	hx := keyHex(key)
	renamed := filepath.Join(dir, shardDirs[ShardA], fmt.Sprintf("%s.g2", hx))
	if err := os.WriteFile(renamed, []byte("garbage-gen2-A"), 0o644); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, shardDirs[ShardB], ".put-g2-abcd-1")
	if err := os.WriteFile(tmp, []byte("garbage-gen2-B"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Also a stray staged repair temp from an even older interrupted repair.
	stray := filepath.Join(dir, shardDirs[ShardParity], ".repair-xyztmp")
	if err := os.WriteFile(stray, []byte("stray"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	m, err := s2.Manifest(key)
	if err != nil || m.Gen != gen1 {
		t.Fatalf("published gen = %v (err=%v), want %d", m, err, gen1)
	}
	if got := mustGet(t, s2, key); !bytes.Equal(got, v1) {
		t.Fatalf("data = %q, want %q", got, v1)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files survived recovery", n)
	}
	if _, err := os.Stat(renamed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced gen-2 shard survived recovery: %v", err)
	}
}

func TestTwoShardFailuresUnrecoverable(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(t *testing.T, s *Store, key string, gen int64)
		wantRoles []int
	}{
		{"two missing", func(t *testing.T, s *Store, key string, gen int64) {
			deleteShard(t, s, ShardA, key, gen)
			deleteShard(t, s, ShardB, key, gen)
		}, []int{ShardA, ShardB}},
		{"missing and corrupt", func(t *testing.T, s *Store, key string, gen int64) {
			deleteShard(t, s, ShardParity, key, gen)
			corruptShard(t, s, ShardA, key, gen)
		}, []int{ShardA, ShardParity}},
		{"two corrupt", func(t *testing.T, s *Store, key string, gen int64) {
			corruptShard(t, s, ShardA, key, gen)
			corruptShard(t, s, ShardB, key, gen)
		}, []int{ShardA, ShardB}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestStore(t)
			ctx := context.Background()
			key := "doomed"
			data := bytes.Repeat([]byte("z"), 64)
			gen := mustPut(t, s, key, data, AnyGen)
			tc.mutate(t, s, key, gen)

			if _, err := s.Repair(ctx, key); !errors.Is(err, ErrUnrecoverable) {
				t.Fatalf("Repair err = %v, want ErrUnrecoverable", err)
			}
			if _, err := s.Get(ctx, key); !errors.Is(err, ErrUnrecoverable) {
				t.Fatalf("Get err = %v, want ErrUnrecoverable", err)
			}
			// Failed read must not have mutated anything.
			if _, err := os.Stat(s.shardPath(tc.wantRoles[0], key, gen)); err == nil && tc.name == "two missing" {
				// role A was deleted and must stay deleted
				t.Fatal("missing shard resurrected by failed read?")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// crash before the manifest is published
// ---------------------------------------------------------------------------

func TestCrashBeforePublish_FirstWrite(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	s1.SetHooks(&Hooks{
		BeforeManifestPublish: func(key string, gen int64) error {
			return ErrInjectedCrash
		},
	})
	_, err = s1.Put(context.Background(), "k", []byte("never-published"), AnyGen)
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("Put err = %v, want ErrInjectedCrash", err)
	}

	// Simulate restart on the exact on-disk state.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	if _, err := s2.Get(context.Background(), "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after crash = %v, want ErrNotFound", err)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d temp/debris files remain after recovery", n)
	}
	for _, rel := range listRegular(t, dir) {
		if strings.Contains(rel, "data0") || strings.Contains(rel, "data1") || strings.Contains(rel, "parity") {
			t.Fatalf("unpublished shard survived recovery: %s", rel)
		}
	}
}

func TestCrashBeforePublish_OverwriteKeepsOldGeneration(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "doc"
	v1 := []byte("version-one")
	mustPut(t, s1, key, v1, AnyGen)

	// Crash only while publishing gen 2.
	s1.SetHooks(&Hooks{
		BeforeManifestPublish: func(k string, gen int64) error {
			if gen == 2 {
				return ErrInjectedCrash
			}
			return nil
		},
	})
	if _, err := s1.Put(ctx, key, []byte("version-two"), 1); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("expected injected crash, got %v", err)
	}

	// Restart: gen 2 staged shards must be purged, gen 1 shards preserved and
	// still readable.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	m, err := s2.Manifest(key)
	if err != nil {
		t.Fatalf("old manifest lost: %v", err)
	}
	if m.Gen != 1 {
		t.Fatalf("manifest gen = %d, want 1", m.Gen)
	}
	if got := mustGet(t, s2, key); !bytes.Equal(got, v1) {
		t.Fatalf("old data = %q, want %q", got, v1)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files remain", n)
	}
	// A normal write must work after recovery and publish gen 2.
	gen := mustPut(t, s2, key, []byte("version-two-retry"), AnyGen)
	if gen != 2 {
		t.Fatalf("gen after recovery = %d, want 2", gen)
	}
}

// Damaged-but-referenced shards must survive restart so the object remains
// repairable afterwards.
func TestRestartPreservesReferencedButCorruptShards(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := "doc"
	data := bytes.Repeat([]byte("persist!"), 8)
	gen := mustPut(t, s1, key, data, AnyGen)
	corruptShard(t, s1, ShardA, key, gen)
	s1.Close()

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	// Corrupt referenced shard is still on disk after recovery...
	if _, err := os.Stat(s2.shardPath(ShardA, key, gen)); err != nil {
		t.Fatalf("referenced corrupt shard was deleted during recovery: %v", err)
	}
	// ...and Get heals it.
	if got := mustGet(t, s2, key); !bytes.Equal(got, data) {
		t.Fatalf("data after restart+heal mismatch")
	}
}

// Crash while a repair has staged its temp file but not committed.
func TestCrashDuringRepairStaged(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "doc"
	data := []byte("repair-in-flight-data")
	gen := mustPut(t, s1, key, data, AnyGen)
	deleteShard(t, s1, ShardB, key, gen)

	s1.SetHooks(&Hooks{
		AfterRepairStaged: func(k string, g int64) error {
			return ErrInjectedCrash
		},
	})
	if _, err := s1.Repair(ctx, key); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("Repair err = %v, want injected crash", err)
	}
	// The staged temp repair file is real crash debris.
	var staged int
	for _, rel := range listRegular(t, dir) {
		if strings.HasPrefix(filepath.Base(rel), ".repair-") {
			staged++
		}
	}
	if staged != 1 {
		t.Fatalf("staged repair temp files = %d, want 1", staged)
	}

	// Restart: debris cleaned, gen 1 still missing B but recoverable.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files remain", n)
	}
	if got := mustGet(t, s2, key); !bytes.Equal(got, data) {
		t.Fatalf("data after crash+restart+heal = %q", got)
	}
}

// ---------------------------------------------------------------------------
// repair vs concurrent conditional write: old-generation repair must lose
// ---------------------------------------------------------------------------

// runRepairWithPutDuringStage performs a Repair of gen 1 while a conditional
// write of gen 2 happens in the staging window, and asserts the repair never
// touches gen 2.
func runRepairWithPutDuringStaged(t *testing.T, gen2Expected int64) {
	t.Helper()
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "race"
	v1 := []byte("first")
	v2 := []byte("second-generation-payload")
	gen1 := mustPut(t, s, key, v1, AnyGen)
	deleteShard(t, s, ShardA, key, gen1)

	release := make(chan struct{})
	s.SetHooks(&Hooks{
		AfterRepairStaged: func(k string, g int64) error {
			if g != gen1 {
				t.Errorf("repair staging unexpected gen %d", g)
			}
			// While the repair holds no lock, publish a new generation.
			if g, err := s.Put(ctx, k, v2, gen2Expected); err != nil {
				return fmt.Errorf("concurrent Put failed: %w", err)
			} else if g != 2 {
				t.Errorf("concurrent Put gen = %d, want 2", g)
			}
			close(release)
			return nil
		},
	})

	healed, err := s.Repair(ctx, key)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if healed {
		t.Fatal("stale-generation repair must not report healing")
	}
	select {
	case <-release:
	default:
		t.Fatal("staging hook never ran")
	}

	m, err := s.Manifest(key)
	if err != nil {
		t.Fatal(err)
	}
	if m.Gen != 2 {
		t.Fatalf("manifest gen = %d, want 2", m.Gen)
	}
	if got := mustGet(t, s, key); !bytes.Equal(got, v2) {
		t.Fatalf("data = %q, want new generation %q", got, v2)
	}
	// No gen-1 file may reappear, gen-2 A must be intact and verified.
	if _, err := os.Stat(s.shardPath(ShardA, key, gen1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old-gen shard resurrected by stale repair (err=%v)", err)
	}
	a2, ok := readVerified(s.shardPath(ShardA, key, 2), m.Shards[ShardA].SHA256)
	if !ok || len(a2) == 0 {
		t.Fatal("gen-2 shard A missing or corrupt after stale repair")
	}
	if n := countDebris(t, s.root); n != 0 {
		t.Fatalf("%d debris files after interleaving", n)
	}
}

func TestRepairLosesToConditionalWrite(t *testing.T) {
	runRepairWithPutDuringStaged(t, 1) // conditional on gen 1
}

func TestRepairLosesToUnconditionalWrite(t *testing.T) {
	runRepairWithPutDuringStaged(t, AnyGen)
}

// Reverse window: the new generation is already published before repair even
// starts; Repair must simply observe a healthy new object and do nothing.
func TestRepairAfterGenerationAdvanced(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "k"
	gen1 := mustPut(t, s, key, []byte("v1"), AnyGen)
	deleteShard(t, s, ShardParity, key, gen1) // damage old gen on disk path...
	mustPut(t, s, key, []byte("v2-new"), gen1)
	healed, err := s.Repair(ctx, key)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if healed {
		t.Fatal("Repair healed something although current gen is healthy")
	}
	if got := string(mustGet(t, s, key)); got != "v2-new" {
		t.Fatalf("data = %q", got)
	}
}

// ---------------------------------------------------------------------------
// background sweeper
// ---------------------------------------------------------------------------

func TestSweeperHealsSingleShard(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, WithSweeper(15*time.Millisecond, func(format string, args ...any) {
		t.Logf(format, args...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	key := "bg"
	data := bytes.Repeat([]byte("sweep-me-"), 10)
	gen := mustPut(t, s, key, data, AnyGen)
	deleteShard(t, s, ShardParity, key, gen)

	pollUntil(t, 2*time.Second, func() bool {
		m, err := s.Manifest(key)
		if err != nil {
			return false
		}
		_, ok := readVerified(s.shardPath(ShardParity, key, m.Gen), m.Shards[ShardParity].SHA256)
		return ok
	})
	if got := mustGet(t, s, key); !bytes.Equal(got, data) {
		t.Fatal("data after background heal mismatch")
	}
}

// Background repair interleaved with a conditional write must not overwrite
// the new generation.
func TestSweeperInterleavedWithWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "k"
	v1 := []byte("v1")
	v2 := []byte("v2-during-sweep")
	gen1 := mustPut(t, s, key, v1, AnyGen)
	deleteShard(t, s, ShardB, key, gen1)

	release := make(chan struct{})
	s.SetHooks(&Hooks{
		AfterRepairStaged: func(k string, g int64) error {
			if _, err := s.Put(ctx, k, v2, 1); err != nil {
				return err
			}
			close(release)
			return nil
		},
	})
	if err := s.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	<-release
	if got := mustGet(t, s, key); !bytes.Equal(got, v2) {
		t.Fatalf("data = %q, want %q", got, v2)
	}
	if m, _ := s.Manifest(key); m.Gen != 2 {
		t.Fatalf("gen = %d, want 2", m.Gen)
	}
}

// A doomed object (two bad shards) must be reported by Sweep but not stop the
// sweep from healing healthy objects.
func TestSweepReportsUnrecoverableAndContinues(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	bad := bytes.Repeat([]byte("x"), 32)
	good := bytes.Repeat([]byte("y"), 32)
	badGen := mustPut(t, s, "bad", bad, AnyGen)
	goodGen := mustPut(t, s, "good", good, AnyGen)
	deleteShard(t, s, ShardA, "bad", badGen)
	deleteShard(t, s, ShardB, "bad", badGen)
	deleteShard(t, s, ShardParity, "good", goodGen)

	err := s.Sweep(ctx)
	if !errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("Sweep err = %v, want ErrUnrecoverable", err)
	}
	if got := mustGet(t, s, "good"); !bytes.Equal(got, good) {
		t.Fatal("sweep did not heal the healthy object")
	}
}

// ---------------------------------------------------------------------------
// delete: tombstone generations
// ---------------------------------------------------------------------------

// shardFilesFor returns the regular files under root belonging to key (any
// generation), relative to root.
func shardFilesFor(t *testing.T, root, key string) []string {
	t.Helper()
	prefix := keyHex(key) + ".g"
	var out []string
	for _, rel := range listRegular(t, root) {
		if strings.HasPrefix(filepath.Base(rel), prefix) {
			out = append(out, rel)
		}
	}
	return out
}

func TestDeleteBasicLifecycle(t *testing.T) {
	s, dir := newTestStore(t)
	ctx := context.Background()
	key := "buoy-telemetry"
	data := []byte("position=78.2N,15.6E;battery=87%")
	gen := mustPut(t, s, key, data, AnyGen)

	tombGen, err := s.Delete(ctx, key, gen)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if tombGen != gen+1 {
		t.Fatalf("tombstone gen = %d, want %d", tombGen, gen+1)
	}

	// The delete conclusion is the only visible state.
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	m, err := s.Manifest(key)
	if err != nil {
		t.Fatalf("tombstone manifest missing: %v", err)
	}
	if !m.Tombstone || m.Gen != tombGen {
		t.Fatalf("manifest = %+v, want tombstone at gen %d", m, tombGen)
	}

	// The deleted generation's shards are reclaimed.
	if left := shardFilesFor(t, dir, key); len(left) != 0 {
		t.Fatalf("shard files remain after delete: %v", left)
	}

	// Repair and the background scan must not "heal" the tombstone generation
	// or resurrect anything into visibility.
	if healed, err := s.Repair(ctx, key); err != nil || healed {
		t.Fatalf("Repair on tombstone: healed=%v err=%v", healed, err)
	}
	if err := s.Sweep(ctx); err != nil {
		t.Fatalf("Sweep with tombstone: %v", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after repair+sweep = %v, want ErrNotFound", err)
	}
	if left := shardFilesFor(t, dir, key); len(left) != 0 {
		t.Fatalf("repair/sweep resurrected shard files: %v", left)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files after delete", n)
	}
}

// A generation conflict or a failed publication must change neither the
// readable data nor the on-disk file set.
func TestDeleteConditionalAndFailureKeepState(t *testing.T) {
	s, dir := newTestStore(t)
	ctx := context.Background()
	key := "doc"
	v1 := []byte("version-one")
	mustPut(t, s, key, v1, AnyGen) // gen 1
	filesBefore := listRegular(t, dir)

	// Wrong preconditions: conflict, nothing changes.
	for _, bad := range []int64{0, 2, 7} {
		if _, err := s.Delete(ctx, key, bad); !errors.Is(err, ErrGenConflict) {
			t.Fatalf("Delete(expectedGen=%d) = %v, want ErrGenConflict", bad, err)
		}
	}
	// Deleting a key that never existed.
	if _, err := s.Delete(ctx, "nope", AnyGen); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete missing key = %v, want ErrNotFound", err)
	}
	// A failed publication (injected) also leaves everything untouched.
	boom := errors.New("publish boom")
	s.SetHooks(&Hooks{BeforeManifestPublish: func(k string, g int64) error { return boom }})
	if _, err := s.Delete(ctx, key, 1); !errors.Is(err, boom) {
		t.Fatalf("Delete with failing publish = %v, want boom", err)
	}
	s.SetHooks(nil)

	if got := mustGet(t, s, key); !bytes.Equal(got, v1) {
		t.Fatal("failed deletes changed readable data")
	}
	if filesAfter := listRegular(t, dir); !slices.Equal(filesBefore, filesAfter) {
		t.Fatalf("file set changed by failed deletes:\nbefore %v\nafter  %v", filesBefore, filesAfter)
	}

	// The real delete works, and a second delete reports the object as gone.
	tombGen, err := s.Delete(ctx, key, 1)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Delete(ctx, key, AnyGen); !errors.Is(err, ErrNotFound) {
		t.Fatalf("re-delete = %v, want ErrNotFound", err)
	}
	if _, err := s.Delete(ctx, key, tombGen); !errors.Is(err, ErrNotFound) {
		t.Fatalf("re-delete with tombstone gen = %v, want ErrNotFound", err)
	}
}

// The same key may be written again after a delete, but the generation must
// not regress: the tombstone occupies the sequence.
func TestDeleteThenRewriteMonotonicGeneration(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "doc"
	mustPut(t, s, key, []byte("v1"), AnyGen) // gen 1
	tombGen, err := s.Delete(ctx, key, 1)    // tombstone gen 2
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Puts based on a pre-delete generation must fail.
	if _, err := s.Put(ctx, key, []byte("zombie"), 1); !errors.Is(err, ErrGenConflict) {
		t.Fatalf("Put on pre-delete gen = %v, want ErrGenConflict", err)
	}
	// Create-only (expectedGen 0) also fails: the tombstone occupies gen 2.
	if _, err := s.Put(ctx, key, []byte("zombie"), 0); !errors.Is(err, ErrGenConflict) {
		t.Fatalf("Put expectedGen=0 over tombstone = %v, want ErrGenConflict", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed puts changed visibility: %v", err)
	}

	// Conditional on the tombstone generation: new object at gen 3.
	gen3, err := s.Put(ctx, key, []byte("v3"), tombGen)
	if err != nil {
		t.Fatalf("Put over tombstone: %v", err)
	}
	if gen3 != tombGen+1 {
		t.Fatalf("rewrite gen = %d, want %d (no regression past tombstone)", gen3, tombGen+1)
	}
	if got := mustGet(t, s, key); string(got) != "v3" {
		t.Fatalf("rewritten data = %q", got)
	}
	// Unconditional writes continue the same monotonic sequence.
	gen4 := mustPut(t, s, key, []byte("v4"), AnyGen)
	if gen4 != gen3+1 {
		t.Fatalf("gen after rewrite = %d, want %d", gen4, gen3+1)
	}
	// And the rewritten object can itself be deleted at its new generation.
	if _, err := s.Delete(ctx, key, gen4); err != nil {
		t.Fatalf("Delete of rewritten object: %v", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after second delete = %v, want ErrNotFound", err)
	}
}

// Crash before the tombstone is published: the old object must survive
// completely readable, and a retried delete succeeds.
func TestDeleteCrashBeforeTombstonePublish(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "doc"
	v1 := []byte("still-here")
	gen1 := mustPut(t, s1, key, v1, AnyGen)

	s1.SetHooks(&Hooks{
		BeforeManifestPublish: func(k string, g int64) error {
			return ErrInjectedCrash
		},
	})
	if _, err := s1.Delete(ctx, key, gen1); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("Delete err = %v, want injected crash", err)
	}

	// Restart on the exact on-disk state: no tombstone was published.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	t.Cleanup(s2.Close)
	m, err := s2.Manifest(key)
	if err != nil {
		t.Fatalf("old manifest lost: %v", err)
	}
	if m.Tombstone || m.Gen != gen1 {
		t.Fatalf("manifest = %+v, want data manifest at gen %d", m, gen1)
	}
	if got := mustGet(t, s2, key); !bytes.Equal(got, v1) {
		t.Fatalf("old data = %q, want %q", got, v1)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files remain after recovery", n)
	}
	// A retried delete publishes the tombstone at gen 2.
	tombGen, err := s2.Delete(ctx, key, gen1)
	if err != nil {
		t.Fatalf("retried Delete: %v", err)
	}
	if tombGen != gen1+1 {
		t.Fatalf("tombstone gen = %d, want %d", tombGen, gen1+1)
	}
	if _, err := s2.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after retried delete = %v, want ErrNotFound", err)
	}
}

// Crash after the tombstone is published but before the old shards are
// reclaimed: only the deletion is visible; recovery collects the leftovers.
func TestDeleteCrashAfterTombstonePublish(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "doc"
	v1 := []byte("doomed-data")
	gen1 := mustPut(t, s1, key, v1, AnyGen)

	s1.SetHooks(&Hooks{
		AfterManifestPublish: func(k string, g int64) error {
			return ErrInjectedCrash
		},
	})
	if _, err := s1.Delete(ctx, key, gen1); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("Delete err = %v, want injected crash", err)
	}

	// The tombstone rename already happened: the manifest on disk is the
	// delete conclusion, and the not-yet-reclaimed old shards are debris.
	m, err := readManifestAt(filepath.Join(dir, "meta", keyHex(key)+".json"))
	if err != nil {
		t.Fatalf("read manifest after crash: %v", err)
	}
	if !m.Tombstone || m.Gen != gen1+1 {
		t.Fatalf("manifest = %+v, want tombstone at gen %d", m, gen1+1)
	}
	for role := 0; role < numShards; role++ {
		if _, err := os.Stat(s1.shardPath(role, key, gen1)); err != nil {
			t.Fatalf("old shard role %d should linger until recovery: %v", role, err)
		}
	}

	// Restart: recovery reclaims the deleted generation's shards; the
	// tombstone stays the only visible state.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	t.Cleanup(s2.Close)
	if _, err := s2.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after crash+restart = %v, want ErrNotFound", err)
	}
	if left := shardFilesFor(t, dir, key); len(left) != 0 {
		t.Fatalf("deleted generation shards survived recovery: %v", left)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files remain after recovery", n)
	}
	// The key may be rewritten; the generation continues past the tombstone.
	gen3 := mustPut(t, s2, key, []byte("reborn"), AnyGen)
	if gen3 != gen1+2 {
		t.Fatalf("rewrite gen = %d, want %d", gen3, gen1+2)
	}
	if got := mustGet(t, s2, key); string(got) != "reborn" {
		t.Fatalf("rewritten data = %q", got)
	}
}

// Crash after a data manifest is published but before the previous
// generation's shards are reclaimed: the new generation is fully readable and
// the old shards are collected as debris.
func TestPutCrashAfterPublishKeepsNewGeneration(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "doc"
	v1 := []byte("v1")
	gen1 := mustPut(t, s1, key, v1, AnyGen)

	s1.SetHooks(&Hooks{
		AfterManifestPublish: func(k string, g int64) error {
			if g == 2 {
				return ErrInjectedCrash
			}
			return nil
		},
	})
	v2 := []byte("v2-published")
	if _, err := s1.Put(ctx, key, v2, gen1); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("Put err = %v, want injected crash", err)
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	t.Cleanup(s2.Close)
	m, err := s2.Manifest(key)
	if err != nil || m.Gen != 2 || m.Tombstone {
		t.Fatalf("manifest = %+v err=%v, want data manifest at gen 2", m, err)
	}
	if got := mustGet(t, s2, key); !bytes.Equal(got, v2) {
		t.Fatalf("data = %q, want %q", got, v2)
	}
	if left := shardFilesFor(t, dir, key); len(left) != numShards {
		t.Fatalf("shard files after recovery = %v, want exactly gen-2 shards", left)
	}
	if n := countDebris(t, dir); n != 0 {
		t.Fatalf("%d debris files remain after recovery", n)
	}
}

// A repair staged against the pre-delete generation must be discarded when a
// delete publishes a tombstone inside the staging window.
func TestRepairStagedThenDeleteDiscardsRepair(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "race"
	v1 := []byte("about-to-be-deleted")
	gen1 := mustPut(t, s, key, v1, AnyGen)
	deleteShard(t, s, ShardA, key, gen1)

	release := make(chan struct{})
	s.SetHooks(&Hooks{
		AfterRepairStaged: func(k string, g int64) error {
			if g != gen1 {
				t.Errorf("repair staging unexpected gen %d", g)
			}
			// While the repair holds no lock, delete the object.
			tombGen, err := s.Delete(ctx, k, gen1)
			if err != nil {
				return fmt.Errorf("concurrent Delete failed: %w", err)
			}
			if tombGen != gen1+1 {
				t.Errorf("concurrent Delete tombstone gen = %d, want %d", tombGen, gen1+1)
			}
			close(release)
			return nil
		},
	})

	healed, err := s.Repair(ctx, key)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if healed {
		t.Fatal("stale-generation repair must not report healing over a tombstone")
	}
	select {
	case <-release:
	default:
		t.Fatal("staging hook never ran")
	}

	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	m, err := s.Manifest(key)
	if err != nil || !m.Tombstone || m.Gen != gen1+1 {
		t.Fatalf("manifest = %+v err=%v, want tombstone at gen %d", m, err, gen1+1)
	}
	// The staged repair must not have resurrected shard A of the deleted
	// generation, and its temp file must be gone.
	if left := shardFilesFor(t, s.root, key); len(left) != 0 {
		t.Fatalf("deleted generation shard resurrected: %v", left)
	}
	if n := countDebris(t, s.root); n != 0 {
		t.Fatalf("%d debris files after interleaving", n)
	}
}

// A repair staged against gen 1 stays discarded even when the staging window
// contains a full delete + rewrite cycle: the commit must not touch gen 3.
func TestRepairStagedAcrossDeleteAndRewrite(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "race"
	v1 := []byte("gen-one-data")
	gen1 := mustPut(t, s, key, v1, AnyGen)
	deleteShard(t, s, ShardB, key, gen1)

	s.SetHooks(&Hooks{
		AfterRepairStaged: func(k string, g int64) error {
			tombGen, err := s.Delete(ctx, k, gen1)
			if err != nil {
				return fmt.Errorf("delete in staging window: %w", err)
			}
			g3, err := s.Put(ctx, k, []byte("gen-three-data"), tombGen)
			if err != nil {
				return fmt.Errorf("rewrite in staging window: %w", err)
			}
			if g3 != gen1+2 {
				t.Errorf("rewrite gen = %d, want %d", g3, gen1+2)
			}
			return nil
		},
	})

	healed, err := s.Repair(ctx, key)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if healed {
		t.Fatal("stale repair reported healing across delete+rewrite")
	}
	if got := mustGet(t, s, key); string(got) != "gen-three-data" {
		t.Fatal("gen-3 data damaged by stale repair")
	}
	m, err := s.Manifest(key)
	if err != nil || m.Gen != gen1+2 || m.Tombstone {
		t.Fatalf("manifest = %+v err=%v, want data manifest at gen %d", m, err, gen1+2)
	}
	// Only gen-3 shards plus the manifest may remain.
	if left := shardFilesFor(t, s.root, key); len(left) != numShards {
		t.Fatalf("unexpected shard files: %v", left)
	}
	if n := countDebris(t, s.root); n != 0 {
		t.Fatalf("%d debris files after interleaving", n)
	}
}

// Reverse window: the delete completed before Repair even starts; Repair is a
// no-op and nothing comes back.
func TestRepairAfterDeleteIsNoOp(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "k"
	gen1 := mustPut(t, s, key, []byte("v1"), AnyGen)
	if _, err := s.Delete(ctx, key, gen1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	healed, err := s.Repair(ctx, key)
	if err != nil || healed {
		t.Fatalf("Repair after delete: healed=%v err=%v", healed, err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
}

// The background sweeper must neither heal nor resurrect a deleted object,
// even while stray shards of the deleted generation still litter the disk
// (e.g. from a crash-interrupted reclaim).
func TestSweeperIgnoresTombstone(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, WithSweeper(10*time.Millisecond, func(format string, args ...any) {
		t.Logf(format, args...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "bg"
	gen1 := mustPut(t, s, key, bytes.Repeat([]byte("p"), 40), AnyGen)
	if _, err := s.Delete(ctx, key, gen1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// A stray shard of the deleted generation, as left behind by a crash
	// between tombstone publication and reclaim.
	stray := s.shardPath(ShardA, key, gen1)
	if err := os.WriteFile(stray, []byte("stale-shard-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Let the sweeper run several passes.
	time.Sleep(150 * time.Millisecond)
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	m, err := s.Manifest(key)
	if err != nil || !m.Tombstone {
		t.Fatalf("manifest = %+v err=%v, want tombstone", m, err)
	}
	s.Close()

	// Restart: recovery reclaims the stray shard; the tombstone persists.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(s2.Close)
	if _, err := os.Stat(stray); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stray shard of deleted generation survived recovery: %v", err)
	}
	if _, err := s2.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after restart = %v, want ErrNotFound", err)
	}
}

// A tombstone references no shards: recovery must remove shard files even when
// their generation number equals the tombstone's — such debris arises when a
// put crashed before publishing its manifest and a delete later published a
// tombstone at that same generation.
func TestRecoveryTombstoneKeepsNoShards(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := "doc"
	gen1 := mustPut(t, s1, key, []byte("v1"), AnyGen)

	// Craft the post-crash layout directly: tombstone manifest at gen 2,
	// stray shards at gen 2 (crashed put) and gen 1 (interrupted reclaim).
	tomb := &Manifest{Key: key, Gen: gen1 + 1, Tombstone: true, UpdatedAt: time.Now().UTC()}
	raw, err := json.Marshal(tomb)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta", keyHex(key)+".json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	for role := 0; role < numShards; role++ {
		if err := os.WriteFile(s1.shardPath(role, key, gen1+1), []byte("stray"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(s2.Close)
	if left := shardFilesFor(t, dir, key); len(left) != 0 {
		t.Fatalf("shard files survived tombstone recovery: %v", left)
	}
	m, err := s2.Manifest(key)
	if err != nil || !m.Tombstone || m.Gen != gen1+1 {
		t.Fatalf("manifest = %+v err=%v, want tombstone at gen %d", m, err, gen1+1)
	}
	if _, err := s2.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
}

// A conditional put and a delete racing on the same generation: exactly one
// wins, the loser gets a generation conflict, and the final state is
// consistent either way.
func TestConcurrentDeleteAndPutSingleWinner(t *testing.T) {
	for round := 0; round < 10; round++ {
		t.Run(fmt.Sprintf("round-%02d", round), func(t *testing.T) {
			s, _ := newTestStore(t)
			ctx := context.Background()
			key := "hot"
			mustPut(t, s, key, []byte("v1"), AnyGen) // gen 1

			var wg sync.WaitGroup
			start := make(chan struct{})
			var putErr, delErr error
			var putGen, delGen int64
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				putGen, putErr = s.Put(ctx, key, []byte("v2"), 1)
			}()
			go func() {
				defer wg.Done()
				<-start
				delGen, delErr = s.Delete(ctx, key, 1)
			}()
			close(start)
			wg.Wait()

			putOK, delOK := putErr == nil, delErr == nil
			if putOK == delOK {
				t.Fatalf("exactly one of put/delete must win: putErr=%v delErr=%v", putErr, delErr)
			}
			if !putOK && !errors.Is(putErr, ErrGenConflict) {
				t.Fatalf("losing put err = %v, want ErrGenConflict", putErr)
			}
			if !delOK && !errors.Is(delErr, ErrGenConflict) {
				t.Fatalf("losing delete err = %v, want ErrGenConflict", delErr)
			}

			if putOK {
				if putGen != 2 {
					t.Fatalf("winning put gen = %d, want 2", putGen)
				}
				if got := mustGet(t, s, key); string(got) != "v2" {
					t.Fatalf("data = %q, want v2", got)
				}
			} else {
				if delGen != 2 {
					t.Fatalf("winning delete tombstone gen = %d, want 2", delGen)
				}
				if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
					t.Fatalf("Get = %v, want ErrNotFound", err)
				}
			}
			if n := countDebris(t, s.root); n != 0 {
				t.Fatalf("%d debris files after race", n)
			}
		})
	}
}
