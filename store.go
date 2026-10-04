// Package shardstore stores each object as two equal-length data shards plus
// one byte-wise XOR parity shard on three local directories.
//
// Layout:
//
//	<root>/data0/<keyhex>.g<gen>   data shard 0  (length ceil(n/2))
//	<root>/data1/<keyhex>.g<gen>   data shard 1  (length ceil(n/2))
//	<root>/parity/<keyhex>.g<gen>  parity shard (length ceil(n/2))
//	<root>/meta/<keyhex>.json      published manifest, rewritten atomically
//
// All publication is conditional on the object generation: a new manifest is
// renamed into place only after all three new shards exist on disk and have
// been re-read and digest-checked, so a half-written generation can never
// become readable. Repair never overwrites shards or the manifest of a newer
// generation; when the manifest has advanced under it, the repair is discarded.
//
// Deletion is itself a published generation: Delete atomically renames a
// tombstone manifest (a generation that references no shards) into place, so
// the delete conclusion has exactly the same crash boundary as a write —
// before the tombstone rename the old object is fully readable, after it only
// the deletion is visible. The tombstone occupies the generation sequence, so
// pre-delete conditional puts and staged repairs lose the generation race,
// and a later rewrite of the same key continues at tombstoneGen+1: generations
// never regress across a delete.
package shardstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Shard roles. The parity shard P satisfies P = A xor B, so any one shard can
// be rebuilt from the other two (missing A = B xor P, missing B = A xor P,
// missing P = A xor B).
const (
	ShardA      = 0
	ShardB      = 1
	ShardParity = 2
	numShards   = 3
)

var shardDirs = [numShards]string{"data0", "data1", "parity"}

// AnyGen may be passed to Put as expectedGen when the caller does not want a
// generation precondition.
const AnyGen = -1

// ErrNotFound means no published manifest exists for the key.
var ErrNotFound = errors.New("shardstore: object not found")

// ErrGenConflict means a conditional Put expected an older generation than
// the one currently published.
var ErrGenConflict = errors.New("shardstore: generation conflict")

// ErrUnrecoverable means two or more shards of the published generation are
// missing or corrupt and the object cannot be reconstructed.
var ErrUnrecoverable = errors.New("shardstore: object unrecoverable (two or more shards bad)")

// ErrInjectedCrash is returned when a test hook simulates a process crash.
// On a real crash no error is returned; under the simulated crash the in-memory
// store stays alive but on-disk state is exactly the post-crash state, so the
// test discards the Store instance and reopens the directory.
var ErrInjectedCrash = errors.New("shardstore: simulated crash")

// ShardInfo describes one shard in a manifest.
type ShardInfo struct {
	Role   int    `json:"role"`
	SHA256 string `json:"sha256"` // lowercase hex digest of shard bytes
}

// Manifest is the published state of an object.
type Manifest struct {
	Key    string       `json:"key"`
	Gen    int64        `json:"gen"` // monotonically increasing object generation
	Length int          `json:"length"`
	Shards [3]ShardInfo `json:"shards"`
	// Tombstone marks a published deletion. A tombstone references no shards;
	// it exists to keep the generation sequence monotonic across a delete so
	// that pre-delete puts and staged repairs lose the generation race and a
	// later rewrite of the key starts above the tombstone's generation.
	Tombstone bool      `json:"tombstone,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Hooks inject faults and observation points for tests. All callbacks are
// optional. Crash hooks are invoked with the per-key lock held, at moments
// that are deliberately not protected against a real process crash:
//
//   - BeforeManifestPublish: after the three new shards were renamed into
//     place and verified, but before the new manifest is published. Also
//     fires before a tombstone manifest is published by Delete.
//   - AfterManifestPublish: after the new manifest (data or tombstone) was
//     atomically published, but before the previous generation's shards are
//     reclaimed. A crash here leaves the old shards as unreferenced debris
//     for startup recovery; the published manifest is already the only
//     visible state.
//   - AfterRepairStaged: after the rebuilt shard of an old generation was
//     staged to a temp file, but before repair re-takes the lock and commits.
type Hooks struct {
	BeforeManifestPublish func(key string, gen int64) error
	AfterManifestPublish  func(key string, gen int64) error
	AfterRepairStaged     func(key string, gen int64) error
}

// Store is a shard store rooted at a single directory.
type Store struct {
	root string

	// keyMu serializes the read-modify-write cycle per object key.
	keyMu keyedMutex

	hooks atomic.Pointer[Hooks]

	stopSweep chan struct{}
	wg        sync.WaitGroup
}

// Option configures a Store.
type Option func(*Store)

// WithSweeper enables a background repair loop that periodically scans every
// published object and heals single-shard damage.
func WithSweeper(interval time.Duration, logf func(format string, args ...any)) Option {
	return func(s *Store) {
		stop := make(chan struct{})
		s.stopSweep = stop
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					if err := s.Sweep(context.Background()); err != nil && logf != nil {
						logf("shardstore sweep: %v", err)
					}
				}
			}
		}()
	}
}

// New creates (or reopens) a store rooted at root and performs crash recovery:
// temp files from interrupted publications and shards not referenced by a
// published manifest are removed, while data still referenced is preserved.
func New(root string, opts ...Option) (*Store, error) {
	s := &Store{root: root}
	for _, d := range shardDirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, fmt.Errorf("shardstore: create dir: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "meta"), 0o755); err != nil {
		return nil, fmt.Errorf("shardstore: create meta dir: %w", err)
	}
	if err := s.recoverOnStartup(); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// SetHooks installs fault-injection hooks. Intended for tests.
func (s *Store) SetHooks(h *Hooks) { s.hooks.Store(h) }

func (s *Store) hook() *Hooks {
	if h := s.hooks.Load(); h != nil {
		return h
	}
	return &Hooks{}
}

// Close stops background workers.
func (s *Store) Close() {
	if s.stopSweep != nil {
		close(s.stopSweep)
	}
	s.wg.Wait()
}

// ---------------------------------------------------------------------------
// paths / encoding
// ---------------------------------------------------------------------------

func keyHex(key string) string { return hex.EncodeToString([]byte(key)) }

func (s *Store) shardPath(role int, key string, gen int64) string {
	return filepath.Join(s.root, shardDirs[role], fmt.Sprintf("%s.g%d", keyHex(key), gen))
}

func (s *Store) manifestPath(key string) string {
	return filepath.Join(s.root, "meta", keyHex(key)+".json")
}

// splitShards cuts data into two equal-length shards. When the object length
// is odd, shard A carries the extra byte and B (and parity) are padded with a
// trailing zero; the manifest Length records the original length so Join can
// strip the padding.
func splitShards(data []byte) (a, b, p []byte) {
	half := (len(data) + 1) / 2
	a = make([]byte, half)
	b = make([]byte, half)
	copy(a, data[:min(half, len(data))])
	copy(b, data[half:])
	p = xorBytes(a, b)
	return a, b, p
}

func joinData(a, b []byte, length int) []byte {
	out := make([]byte, length)
	half := (length + 1) / 2
	copy(out, a[:min(half, length)])
	copy(out[half:], b)
	return out
}

func xorBytes(x, y []byte) []byte {
	if len(x) != len(y) {
		panic(fmt.Sprintf("xorBytes: length mismatch %d != %d", len(x), len(y)))
	}
	out := make([]byte, len(x))
	for i := range x {
		out[i] = x[i] ^ y[i]
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randToken() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func digestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// atomic file writes
// ---------------------------------------------------------------------------

// writeFileAtomic writes data to a temp file in dstDir, fsyncs it and renames
// it onto dst. dst may be in a different directory than the temp file's
// "natural" directory, so dstDir is explicit; callers must fsync dstDir after
// the rename if the rename itself must survive a crash.
func writeFileAtomic(dst, dstDir string, data []byte, perm fs.FileMode) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dstDir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		cleanup()
		return err
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

// ---------------------------------------------------------------------------
// manifest IO
// ---------------------------------------------------------------------------

func readManifestAt(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("shardstore: corrupt manifest %s: %w", path, err)
	}
	return &m, nil
}

func (s *Store) currentManifest(key string) (*Manifest, error) {
	return readManifestAt(s.manifestPath(key))
}

// publishManifest writes the manifest atomically (temp file + fsync + rename +
// dir fsync). The shard directory renames and this manifest rename together
// implement the conditional generation update: readers only ever see a
// manifest whose three shards are complete and verified.
func (s *Store) publishManifest(m *Manifest) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	metaDir := filepath.Join(s.root, "meta")
	if err := writeFileAtomic(s.manifestPath(m.Key), metaDir, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	return syncDir(metaDir)
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

// Put conditionally writes data for key. If expectedGen is AnyGen the write is
// unconditional; otherwise it proceeds only when the published generation
// equals expectedGen. On success the new generation is returned.
//
// The published state the precondition is checked against may be a tombstone
// left by Delete: the generation sequence continues across deletions, so the
// new generation is always one above the published manifest (data or
// tombstone) and never regresses.
//
// The three new shards are staged as temp files, verified, renamed into their
// final names, re-read and digest-checked before the manifest is published.
// Any failure (including an injected crash point) removes the unpublished
// shards so no half-finished generation remains readable.
func (s *Store) Put(ctx context.Context, key string, data []byte, expectedGen int64) (newGen int64, err error) {
	unlock := s.keyMu.lock(key)
	defer unlock()

	cur, err := s.currentManifest(key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return 0, err
	}
	if expectedGen != AnyGen {
		curGen := int64(0)
		if cur != nil {
			curGen = cur.Gen
		}
		if curGen != expectedGen {
			return 0, fmt.Errorf("%w: key %q at gen %d, expected %d", ErrGenConflict, key, curGen, expectedGen)
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	newGeneration := int64(1)
	if cur != nil {
		newGeneration = cur.Gen + 1
	}

	ba, bb, bp := splitShards(data)
	blobs := [numShards][]byte{ba, bb, bp}

	// Stage shards as unique temp files directly in their target dirs so the
	// final rename is atomic on the same filesystem.
	tmpNames := [numShards]string{}
	finalNames := [numShards]string{}
	committedRenames := 0
	crashed := false
	published := false
	defer func() {
		if err == nil || crashed || published {
			// A simulated crash models process death: deferred cleanup does not
			// run, exactly as in a real crash. Startup recovery is responsible
			// for the debris left behind. Once the manifest is published the
			// new shards are referenced state, not debris: they must survive
			// whatever the post-publish hooks report.
			return
		}
		// Remove shards of the failed generation that were already renamed.
		// Nothing references them yet, so they are pure debris.
		for i := 0; i < committedRenames; i++ {
			_ = os.Remove(finalNames[i])
		}
		for i := committedRenames; i < numShards; i++ {
			if tmpNames[i] != "" {
				_ = os.Remove(tmpNames[i])
			}
		}
		if committedRenames > 0 {
			for _, role := range []int{ShardA, ShardB, ShardParity} {
				_ = syncDir(filepath.Join(s.root, shardDirs[role]))
			}
		}
	}()

	for role := 0; role < numShards; role++ {
		dir := filepath.Join(s.root, shardDirs[role])
		final := s.shardPath(role, key, newGeneration)
		tmp := filepath.Join(dir, fmt.Sprintf(".put-g%d-%s-%d", newGeneration, randToken(), role))
		f, cerr := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if cerr != nil {
			err = cerr
			return 0, err
		}
		_, werr := f.Write(blobs[role])
		syncErr := f.Sync()
		_ = f.Close()
		if werr != nil {
			_ = os.Remove(tmp)
			err = werr
			return 0, err
		}
		if syncErr != nil {
			_ = os.Remove(tmp)
			err = syncErr
			return 0, err
		}
		tmpNames[role] = tmp
		finalNames[role] = final
	}

	// Verify staged temp contents before they take a final name.
	for role := 0; role < numShards; role++ {
		got, rerr := os.ReadFile(tmpNames[role])
		if rerr != nil {
			err = rerr
			return 0, err
		}
		if digestHex(got) != digestHex(blobs[role]) || len(got) != len(blobs[role]) {
			err = fmt.Errorf("shardstore: staged shard %d failed verification for key %q gen %d", role, key, newGeneration)
			return 0, err
		}
	}

	for role := 0; role < numShards; role++ {
		if rerr := os.Rename(tmpNames[role], finalNames[role]); rerr != nil {
			err = rerr
			return 0, err
		}
		committedRenames++
		if serr := syncDir(filepath.Join(s.root, shardDirs[role])); serr != nil {
			err = serr
			return 0, err
		}
	}

	// All three shards now have their final names. Re-read and verify them
	// from those names — this is exactly what a later reader will see.
	m := &Manifest{
		Key:       key,
		Gen:       newGeneration,
		Length:    len(data),
		UpdatedAt: time.Now().UTC(),
	}
	for role := 0; role < numShards; role++ {
		got, rerr := os.ReadFile(finalNames[role])
		if rerr != nil {
			err = rerr
			return 0, err
		}
		if digestHex(got) != digestHex(blobs[role]) {
			err = fmt.Errorf("shardstore: final shard %d digest mismatch for key %q gen %d", role, key, newGeneration)
			return 0, err
		}
		m.Shards[role] = ShardInfo{Role: role, SHA256: digestHex(got)}
	}

	// Crash point: shards of gen N are on disk, manifest still points at the
	// previous generation (or does not exist). Recovery must delete these
	// unreferenced shards and keep the previously published data.
	if h := s.hook(); h.BeforeManifestPublish != nil {
		if herr := h.BeforeManifestPublish(key, newGeneration); herr != nil {
			err = herr
			if errors.Is(herr, ErrInjectedCrash) {
				crashed = true
			}
			return 0, err
		}
	}

	if perr := s.publishManifest(m); perr != nil {
		err = perr
		return 0, err
	}
	published = true

	// Crash point: gen-N manifest published, previous generation's shards not
	// yet reclaimed. The new generation is already the only readable state;
	// the old shards are unreferenced debris that startup recovery removes.
	if h := s.hook(); h.AfterManifestPublish != nil {
		if herr := h.AfterManifestPublish(key, newGeneration); herr != nil {
			err = herr
			if errors.Is(herr, ErrInjectedCrash) {
				crashed = true
			}
			return 0, err
		}
	}

	// Publication succeeded; err stays nil so the deferred cleanup skips the
	// new shards. Best-effort removal of the previous generation's shards.
	if cur != nil {
		for role := 0; role < numShards; role++ {
			_ = os.Remove(s.shardPath(role, key, cur.Gen))
		}
		for _, d := range shardDirs {
			_ = syncDir(filepath.Join(s.root, d))
		}
	}
	return newGeneration, nil
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

// Delete conditionally removes key by publishing a tombstone manifest at the
// next generation. If expectedGen is AnyGen the delete is unconditional;
// otherwise it proceeds only when the published generation equals expectedGen.
// Deleting a key with no live object (no manifest at all, or an already
// published tombstone) returns ErrNotFound. A generation conflict or a failed
// publication changes neither the readable data nor the on-disk file set.
//
// The tombstone is published through the same crash-safe boundary as a data
// manifest (temp file + fsync + atomic rename + dir fsync): a crash before
// the rename leaves the old object fully readable, a crash after it exposes
// only the deletion. Once the tombstone is published:
//
//   - Get reports ErrNotFound;
//   - Repair and the sweeper treat the key as having nothing to heal and
//     never resurrect old shards into a visible version;
//   - repairs staged against the pre-delete generation are discarded by the
//     usual generation check, and conditional puts based on a pre-delete
//     generation fail with ErrGenConflict;
//   - the key may be rewritten afterwards: the next Put publishes
//     tombstoneGen+1, so generations never regress across a delete.
//
// The deleted generation's shards are reclaimed best-effort after publication;
// whatever remains is unreferenced debris that startup recovery removes. A
// tombstone references no shards, so recovery never deletes data of a newer
// generation on its behalf.
func (s *Store) Delete(ctx context.Context, key string, expectedGen int64) (newGen int64, err error) {
	unlock := s.keyMu.lock(key)
	defer unlock()

	cur, err := s.currentManifest(key)
	if err != nil {
		return 0, err // ErrNotFound when nothing was ever published
	}
	if cur.Tombstone {
		return 0, fmt.Errorf("%w: key %q already deleted at gen %d", ErrNotFound, key, cur.Gen)
	}
	if expectedGen != AnyGen && cur.Gen != expectedGen {
		return 0, fmt.Errorf("%w: key %q at gen %d, expected %d", ErrGenConflict, key, cur.Gen, expectedGen)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	tombGen := cur.Gen + 1
	tomb := &Manifest{
		Key:       key,
		Gen:       tombGen,
		Tombstone: true,
		UpdatedAt: time.Now().UTC(),
	}

	// Crash point: tombstone not yet published; the old object is still the
	// only visible state and remains fully readable after a restart.
	if h := s.hook(); h.BeforeManifestPublish != nil {
		if herr := h.BeforeManifestPublish(key, tombGen); herr != nil {
			return 0, herr
		}
	}

	if perr := s.publishManifest(tomb); perr != nil {
		return 0, perr
	}

	// Crash point: tombstone published, old shards not yet reclaimed. The
	// deletion is already the only visible conclusion; the leftover shards
	// are unreferenced debris that startup recovery removes.
	if h := s.hook(); h.AfterManifestPublish != nil {
		if herr := h.AfterManifestPublish(key, tombGen); herr != nil {
			return 0, herr
		}
	}

	// Best-effort reclaim of the deleted generation's shards. The names are
	// generation-scoped, so this cannot touch data of any newer generation.
	for role := 0; role < numShards; role++ {
		_ = os.Remove(s.shardPath(role, key, cur.Gen))
	}
	for _, d := range shardDirs {
		_ = syncDir(filepath.Join(s.root, d))
	}
	return tombGen, nil
}

// ---------------------------------------------------------------------------
// read / verify / repair
// ---------------------------------------------------------------------------

// readVerified loads and digest-checks one shard. Returns (nil, true) when the
// shard is missing or corrupt.
func readVerified(path, wantDigest string) ([]byte, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	if wantDigest != "" && digestHex(raw) != wantDigest {
		return nil, false
	}
	return raw, true
}

// rebuild reconstructs the shard at badRole from two good shards of identical
// length, and verifies it against wantDigest.
func rebuild(badRole int, x, y []byte, wantDigest string) ([]byte, error) {
	if len(x) != len(y) {
		return nil, fmt.Errorf("shardstore: good shard length mismatch %d != %d", len(x), len(y))
	}
	rec := xorBytes(x, y)
	if digestHex(rec) != wantDigest {
		return nil, fmt.Errorf("shardstore: rebuilt shard %d digest mismatch", badRole)
	}
	return rec, nil
}

// Get returns the object data for key, rebuilding and healing exactly one bad
// shard if needed. Two or more bad shards return ErrUnrecoverable. A key whose
// published manifest is a tombstone (deleted) reports ErrNotFound, exactly
// like a key that never existed.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	unlock := s.keyMu.lock(key)
	defer unlock()

	m, err := s.currentManifest(key)
	if err != nil {
		return nil, err
	}
	if m.Tombstone {
		return nil, fmt.Errorf("%w: key %q deleted at gen %d", ErrNotFound, key, m.Gen)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, _, err := s.assembleLocked(m, true)
	return data, err
}

// assembleLocked reads the three shards described by m, reconstructs the
// object and optionally heals a single bad shard in place. It returns the data
// and the number of bad shards found.
func (s *Store) assembleLocked(m *Manifest, heal bool) (data []byte, badCount int, err error) {
	raw := [numShards][]byte{}
	badRoles := make([]int, 0, numShards)
	for role := 0; role < numShards; role++ {
		b, ok := readVerified(s.shardPath(role, m.Key, m.Gen), m.Shards[role].SHA256)
		if ok {
			raw[role] = b
		} else {
			badRoles = append(badRoles, role)
		}
	}
	badCount = len(badRoles)
	if badCount >= 2 {
		return nil, badCount, fmt.Errorf("%w: key %q gen %d, bad shard roles %v", ErrUnrecoverable, m.Key, m.Gen, badRoles)
	}

	if badCount == 1 {
		badRole := badRoles[0]
		// Pick any two other roles; both must be good since exactly one is bad.
		var xRole, yRole int
		switch badRole {
		case ShardA:
			xRole, yRole = ShardB, ShardParity
		case ShardB:
			xRole, yRole = ShardA, ShardParity
		default:
			xRole, yRole = ShardA, ShardB
		}
		rec, rerr := rebuild(badRole, raw[xRole], raw[yRole], m.Shards[badRole].SHA256)
		if rerr != nil {
			// Reconstruction that fails verification is indistinguishable from
			// a second bad shard.
			return nil, badCount + 1, fmt.Errorf("%w: key %q gen %d: %v", ErrUnrecoverable, m.Key, m.Gen, rerr)
		}
		raw[badRole] = rec
		if heal {
			if werr := s.healShardLocked(badRole, m, rec); werr != nil {
				// Data is reconstructable; return it but surface the write-back
				// failure to the caller.
				return joinData(raw[ShardA], raw[ShardB], m.Length), badCount, werr
			}
		}
	}

	return joinData(raw[ShardA], raw[ShardB], m.Length), badCount, nil
}

// healShardLocked writes rebuilt bytes onto the shard path of exactly the
// generation named in m, atomically, and verifies the result from its final
// name. Caller holds the per-key lock.
func (s *Store) healShardLocked(role int, m *Manifest, rebuilt []byte) error {
	final := s.shardPath(role, m.Key, m.Gen)
	if err := writeFileAtomic(final, filepath.Join(s.root, shardDirs[role]), rebuilt, 0o644); err != nil {
		return err
	}
	if err := syncDir(filepath.Join(s.root, shardDirs[role])); err != nil {
		return err
	}
	got, err := os.ReadFile(final)
	if err != nil {
		return err
	}
	if digestHex(got) != m.Shards[role].SHA256 {
		return fmt.Errorf("shardstore: repaired shard failed verification on disk (role %d, key %q, gen %d)", role, m.Key, m.Gen)
	}
	return nil
}

// Repair heals one missing or corrupt shard of the published generation. It is
// safe to run while writes are in progress: the rebuilt shard is staged while
// no lock is held, and the commit only proceeds if the manifest still names
// the same generation. A repair of an old generation is discarded rather than
// overwriting the newer generation's shards.
//
// A key whose published manifest is a tombstone has nothing to heal: Repair
// reports (false, nil) and never touches the deleted generation's leftover
// shards, so a deleted object cannot be resurrected into a visible version.
//
// It returns (true, nil) when a shard was healed, (false, nil) when nothing
// was wrong, and ErrUnrecoverable when two or more shards are bad.
func (s *Store) Repair(ctx context.Context, key string) (healed bool, err error) {
	// Phase 1: snapshot under the lock.
	unlock := s.keyMu.lock(key)
	m, err := s.currentManifest(key)
	if err != nil {
		unlock()
		return false, err
	}
	if m.Tombstone {
		// Deleted: no shards are referenced, so there is nothing to rebuild
		// and nothing that may be written back.
		unlock()
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		unlock()
		return false, err
	}
	raw := [numShards][]byte{}
	badRoles := make([]int, 0, numShards)
	for role := 0; role < numShards; role++ {
		b, ok := readVerified(s.shardPath(role, key, m.Gen), m.Shards[role].SHA256)
		if ok {
			raw[role] = b
		} else {
			badRoles = append(badRoles, role)
		}
	}
	if len(badRoles) == 0 {
		unlock()
		return false, nil
	}
	if len(badRoles) >= 2 {
		unlock()
		return false, fmt.Errorf("%w: key %q gen %d, bad shard roles %v", ErrUnrecoverable, key, m.Gen, badRoles)
	}
	badRole := badRoles[0]
	var xRole, yRole int
	switch badRole {
	case ShardA:
		xRole, yRole = ShardB, ShardParity
	case ShardB:
		xRole, yRole = ShardA, ShardParity
	default:
		xRole, yRole = ShardA, ShardB
	}
	snapshotGen := m.Gen
	snapshotM := *m
	rebuilt, rerr := rebuild(badRole, raw[xRole], raw[yRole], m.Shards[badRole].SHA256)
	unlock()
	if rerr != nil {
		return false, fmt.Errorf("%w: key %q gen %d: %v", ErrUnrecoverable, key, snapshotGen, rerr)
	}

	// Phase 2 (no lock held): a concurrent Put may publish a newer generation
	// here. Stage the rebuilt bytes as a uniquely named temp file in the
	// shard's directory; this touches nothing of any published generation.
	dir := filepath.Join(s.root, shardDirs[badRole])
	tmp, err := os.CreateTemp(dir, ".repair-*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	crashed := false
	defer func() {
		// Under a simulated crash the temp file is left behind for startup
		// recovery, like a real killed process.
		if !crashed {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(rebuilt); err != nil {
		_ = tmp.Close()
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}

	if h := s.hook(); h.AfterRepairStaged != nil {
		if herr := h.AfterRepairStaged(key, snapshotGen); herr != nil {
			if errors.Is(herr, ErrInjectedCrash) {
				crashed = true
			}
			return false, herr
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}

	// Phase 3: re-take the lock and commit only if the generation is unchanged.
	unlock = s.keyMu.lock(key)
	defer unlock()

	cur, err := s.currentManifest(key)
	if err != nil {
		return false, err
	}
	if cur.Gen != snapshotGen {
		// A newer generation was published while we worked. The staged shard
		// belongs to the old generation: drop it entirely.
		return false, nil
	}
	if cur.Shards != snapshotM.Shards || cur.Length != snapshotM.Length {
		return false, nil
	}
	// Re-check the target: another repair (e.g. read-heal) may have fixed it.
	if _, ok := readVerified(s.shardPath(badRole, key, cur.Gen), cur.Shards[badRole].SHA256); ok {
		return false, nil
	}
	if err := s.commitRepairLocked(badRole, cur, tmpName); err != nil {
		return false, err
	}
	return true, nil
}

// commitRepairLocked links the staged temp file to its final generation-scoped
// name and verifies it. The temp name carries only the old rebuilt bytes, so
// even though os.Rename replaces the destination it can only replace a file of
// the same generation asserted under the lock.
func (s *Store) commitRepairLocked(role int, m *Manifest, tmpName string) error {
	final := s.shardPath(role, m.Key, m.Gen)
	if err := os.Rename(tmpName, final); err != nil {
		return err
	}
	if err := syncDir(filepath.Join(s.root, shardDirs[role])); err != nil {
		return err
	}
	got, err := os.ReadFile(final)
	if err != nil {
		return err
	}
	if digestHex(got) != m.Shards[role].SHA256 {
		_ = os.Remove(final)
		return fmt.Errorf("shardstore: committed repair failed verification (role %d, gen %d)", role, m.Gen)
	}
	return nil
}

// Manifest returns a copy of the published manifest for key, if any. The
// returned manifest may be a tombstone: deletion is a published generation.
func (s *Store) Manifest(key string) (*Manifest, error) {
	unlock := s.keyMu.lock(key)
	defer unlock()
	m, err := s.currentManifest(key)
	if err != nil {
		return nil, err
	}
	cp := *m
	return &cp, nil
}

// ---------------------------------------------------------------------------
// sweeper
// ---------------------------------------------------------------------------

// Sweep scans all published objects and heals single-shard damage. Objects
// with two bad shards are reported together; the sweep continues with the rest.
func (s *Store) Sweep(ctx context.Context) error {
	keys, err := s.listManifestKeys()
	if err != nil {
		return err
	}
	var errs []error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, rerr := s.Repair(ctx, key); rerr != nil {
			if errors.Is(rerr, ErrUnrecoverable) {
				errs = append(errs, rerr)
				continue
			}
			errs = append(errs, rerr)
		}
	}
	return errors.Join(errs...)
}

func (s *Store) listManifestKeys() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "meta"))
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		hexKey := strings.TrimSuffix(name, ".json")
		raw, derr := hex.DecodeString(hexKey)
		if derr != nil {
			continue
		}
		keys = append(keys, string(raw))
	}
	sort.Strings(keys)
	return keys, nil
}

// ---------------------------------------------------------------------------
// crash recovery
// ---------------------------------------------------------------------------

// recoverOnStartup makes on-disk state consistent with the set of published
// manifests:
//
//   - temp files (.* in shard/meta dirs) left by interrupted puts or repairs
//     are removed;
//   - shard files whose <keyhex>.g<gen> name is not referenced by that key's
//     published manifest (including half-written generations crashed before
//     publication) are removed;
//   - shards referenced by a published manifest are preserved, even damaged
//     ones, so the object stays repairable;
//   - a tombstone manifest references no shards at all, so every shard file
//     of a deleted key is reclaimed — including debris whose generation
//     number equals the tombstone's (e.g. a put that crashed before its
//     manifest was published and was later superseded by the tombstone).
func (s *Store) recoverOnStartup() error {
	// keep[shardDir][basename] marks shard files referenced by a published
	// manifest; those must survive recovery even when damaged.
	keep := map[string]map[string]bool{}
	for _, d := range shardDirs {
		keep[d] = map[string]bool{}
	}

	manifests, err := os.ReadDir(filepath.Join(s.root, "meta"))
	if err != nil {
		return err
	}
	for _, e := range manifests {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		hexKey := strings.TrimSuffix(name, ".json")
		if _, derr := hex.DecodeString(hexKey); derr != nil {
			continue // unknown file: leave it alone
		}
		m, merr := readManifestAt(filepath.Join(s.root, "meta", name))
		if merr != nil {
			continue // corrupt manifest: be conservative, delete nothing for it
		}
		if m.Tombstone {
			// A tombstone references no shards: every shard file of this key,
			// of any generation, is reclaimable debris.
			continue
		}
		for role := 0; role < numShards; role++ {
			keep[shardDirs[role]][fmt.Sprintf("%s.g%d", hexKey, m.Gen)] = true
		}
	}

	for _, d := range shardDirs {
		dir := filepath.Join(s.root, d)
		entries, rerr := os.ReadDir(dir)
		if rerr != nil {
			return rerr
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() {
				continue
			}
			if strings.HasPrefix(name, ".") {
				_ = os.Remove(filepath.Join(dir, name))
				continue
			}
			if !keep[d][name] {
				_ = os.Remove(filepath.Join(dir, name))
			}
		}
		if err := syncDir(dir); err != nil {
			return err
		}
	}

	// Remove leftover temp manifests; the last fully renamed JSON is the
	// published one.
	metaDir := filepath.Join(s.root, "meta")
	mentries, err := os.ReadDir(metaDir)
	if err != nil {
		return err
	}
	for _, e := range mentries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), ".") {
			_ = os.Remove(filepath.Join(metaDir, e.Name()))
		}
	}
	return syncDir(metaDir)
}
