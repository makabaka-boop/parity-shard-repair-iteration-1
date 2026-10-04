package shardstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
