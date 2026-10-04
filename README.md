# shardstore

Local Go object store with 2+1 erasure coding: every object is split into two
equal-length data shards plus one byte-wise XOR parity shard, kept in three
directories:

```
<root>/data0/<key>.g<gen>     data shard A  (ceil(n/2) bytes)
<root>/data1/<key>.g<gen>     data shard B  (ceil(n/2) bytes, zero-padded if n odd)
<root>/parity/<key>.g<gen>    parity P = A xor B
<root>/meta/<key>.json        published manifest (length, gen, per-shard SHA-256, or a tombstone)
```

Any one shard can be rebuilt from the other two
(`A = B xor P`, `B = A xor P`, `P = A xor B`). Losing two shards is reported
as `ErrUnrecoverable`.

## Usage

```go
s, _ := shardstore.New("/var/lib/shards",
    shardstore.WithSweeper(30*time.Second, log.Printf)) // optional background healer

gen, err := s.Put(ctx, "obj1", data, shardstore.AnyGen) // unconditional write
gen, err = s.Put(ctx, "obj1", data2, gen)               // conditional write (CAS)
data, err := s.Get(ctx, "obj1")                         // read, auto-heals one bad shard
healed, err := s.Repair(ctx, "obj1")                    // explicit repair
err = s.Delete(ctx, "obj1", gen)                        // conditional delete (tombstone)
```

## Guarantees

- **Conditional generation publication.** `Put(key, data, expectedGen)` fails
  with `ErrGenConflict` unless the published generation equals `expectedGen`
  (`AnyGen` = unconditional). The new manifest is published by
  temp-file + fsync + atomic rename only after all three new shards exist with
  their final names and have been re-read from disk and digest-verified. A
  half-written generation is therefore never readable.
- **Conditional tombstone deletes.** `Delete(key, expectedGen)` uses the same
  generation precondition as `Put` and publishes a new, higher-generation
  manifest with `deleted: true`. `Get` then returns `ErrNotFound`; `Repair` and
  `Sweep` do not resurrect or reconstruct deleted shards. A `Put` based on the
  pre-delete generation fails, while a `Put` based on the tombstone generation
  (or `AnyGen`) succeeds only at a still higher generation. Deleting a missing
  key is idempotent with `AnyGen`.
- **Read-path rebuild and heal.** `Get`/`Repair` digest-check every shard.
  Exactly one missing/corrupt shard is rebuilt from the other two, the rebuilt
  bytes themselves verified against the manifest digest, then written back
  atomically. Two bad shards return `ErrUnrecoverable` without mutating disk.
- **Old-generation repair never clobbers new data.** `Repair` snapshots the
  manifest + good shards, stages the rebuilt shard as a temp file without
  holding the lock, and only commits after re-taking the lock and confirming
  the same live generation is still published. If a concurrent write or delete
  published a newer generation, the staged repair is discarded (its temp file
  deleted), so newer shards, newer manifests, and tombstones cannot be
  overwritten.
- **Crash recovery on open.** Reopening the root removes all dotfile temp
  debris (interrupted puts/repairs) and every shard file not referenced by a
  published live manifest. Shards referenced by a live manifest are kept even
  when damaged, so single-shard failures remain repairable after restart; a
  tombstone keeps no shards, so deleted shards are reclaimed.
- **Durability ordering.** Each shard file is fsynced before its rename, each
  directory is fsynced after renames, and the manifest fsync/fsync-dir is the
  final step — so the only crash windows are "old manifest visible" or "new
  manifest visible with complete shards" (for a delete: tombstone visible),
  never a torn publication. Superseded shards are reclaimed only after that
  manifest publication; recovery removes them without removing newer
  generations.

## Fault injection (tests)

`Store.SetHooks` provides two crash points used by the test suite:

- `BeforeManifestPublish` — all three gen-N shards are on disk for a Put, the
  manifest still points at the previous generation; for a Delete the old object
  and tombstone predecessor are unchanged (verifies crash-before-publish +
  restart recovery for writes and deletes);
- `AfterManifestPublish` — the object or tombstone manifest is durably
  published but superseded shards have not yet been reclaimed (verifies that
  post-publication crashes preserve the publication conclusion and restart
  cleanup does not delete newer data);
- `AfterRepairStaged` — the rebuilt shard is staged but uncommitted, allowing a
  conditional `Put` or `Delete`+rewrite to publish a newer generation inside
  the repair window (verifies the stale repair is dropped and new data is
  untouched).

Tests also cover single-volume loss/bit-rot for each shard role, two-shard
loss, odd-length padding, 16 concurrent conditional writers with exactly one
winner, the background sweeper, and a manually constructed mid-rename crash
layout. Run with:

```sh
go test -race ./...
```
