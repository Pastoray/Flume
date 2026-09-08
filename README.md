# Flume

A production-grade, embedded key-value store implemented from scratch in
Go, built around an LSM-tree (Log-Structured Merge-tree) storage engine
with leveled compaction, range scans, point-in-time snapshots, optional
value compression, and an HTTP service mode. No third-party KV, storage,
web framework, or RPC libraries are used - everything is built on the Go
standard library.

## Architecture

```
Put/Delete
    │
    ▼
┌─────────────┐     fsync'd, checksummed       ┌──────────────┐
│     WAL      │ ───────────────────────────▶ │  active file  │
└─────────────┘                                └──────────────┘
    │ (only after WAL fsync succeeds)
    ▼
┌─────────────────┐   size threshold    ┌────────────────────────┐
│  active MemTable │ ──────────────────▶│ immutable MemTable(s)   │
│ (concurrent      │      exceeded       │ awaiting flush          │
│  skip list)       │                    └────────────────────────┘
└─────────────────┘                                │
    │ Get()/Scan() check here first                 │ single flush worker,
    ▼                                                │ strictly FIFO, always -> L0
┌─────────────────────────────────────────────────┐  ▼
│  L0  (overlapping, one table per flush)           │◀───────────
├─────────────────────────────────────────────────┤
│  L1  (non-overlapping, ~2 MiB target)             │◀── L0->L1 compaction
├─────────────────────────────────────────────────┤       (L0 count trigger)
│  L2  (non-overlapping, ~20 MiB target)            │◀── L1->L2 compaction
├─────────────────────────────────────────────────┤       (size trigger, x10/level)
│  ... up to L6 by default                          │
└─────────────────────────────────────────────────┘
```

### Components

| Package     | Responsibility |
|-------------|----------------|
| `wal`       | Append-only, checksummed (CRC32) write-ahead log. Every record is fsynced before the caller is told the write is durable. `Replay` recovers as much as possible from a torn/corrupted tail without failing. |
| `skiplist`  | Generic, concurrency-safe ordered map (`SkipList[V any]`) guarded by a single `sync.RWMutex`, with both full (`All`) and bounded (`Range`) iteration. Backs the MemTable. |
| `memtable`  | The mutable, in-memory write buffer. Tracks approximate size to decide when to rotate. |
| `bloom`     | Classic Bloom filter (optimal `m`/`k` sizing, double hashing via two FNV variants) embedded in every SSTable. |
| `sstable`   | On-disk sorted, **multi-version** run format: data block + full index + Bloom filter + footer, with optional per-value DEFLATE compression. `Merge` performs the k-way compaction merge with snapshot-aware version retention. |
| `engine`    | Ties everything together: WAL-then-MemTable writes, MemTable rotation, a single serialized flush worker, leveled background compaction, range scans, and point-in-time snapshots. |
| `server`    | Stdlib-only HTTP/JSON REST API wrapping the engine, for running it as a standalone networked service. |
| `metrics`   | Dependency-free Prometheus-text-format metrics (`Counter`, `Gauge`, `GaugeVec`, latency `Histogram`), used by the engine to instrument itself. |

### On-disk SSTable format

```
[ Data Block   ]  repeated: [keyLen(4)][seq(8)][type(1)][valLen(4)][key][value]
[ Index Block  ]  repeated: [keyLen(4)][key][offset(8)]     (one entry per RECORD)
[ Bloom Filter ]  [numBits(4)][numHash(4)][bit array]
[ Footer       ]  [indexOffset(8)][indexLen(8)][bloomOffset(8)][bloomLen(8)][numEntries(8)][magic(4)]
```

Unlike a plain point-lookup format, a table may hold **more than one
record for the same key**, stored newest-first within that key's group
(sorted by Seq descending). This is what makes snapshot reads possible:
compaction only drops an old version once no open snapshot could still
need it.

Filenames encode the compaction level directly - `L{level:02d}-{gen:020d}.sst`
- so a plain directory listing recovers each table's level without
opening the file; `gen` is purely a unique on-disk id and carries no
recency meaning of its own (see below).

## Features

- **Storage engine**: LSM-tree with WAL, concurrent skip-list MemTable, immutable SSTables.
- **Durability**: strict write-ahead log, fsynced before every write is acknowledged.
- **Crash recovery**: WAL replay on startup, tolerant of a torn/corrupted tail.
- **Concurrency**: fine-grained `sync.RWMutex` throughout; reference-counted SSTable lifetimes so readers never race with compaction's file close/delete.
- **Leveled compaction**: L0 (overlapping, flush target) compacts into L1 once it accumulates too many tables; L1+ compact into the next level once total size crosses a geometrically growing target.
- **Range scans** (`Scan(start, end)`): a streaming, heap-merged iterator across the MemTable, every immutable MemTable, and every SSTable at once - no need to materialize the whole range into memory.
- **Point-in-time snapshots** (`Snapshot()`): `Get`/`Scan` against a snapshot only see writes that had completed when it was taken, built on the same per-write sequence number used for compaction's recency resolution. See the isolation caveat below.
- **Value compression**: optional per-value DEFLATE compression (`WithCompression`), applied only when it actually shrinks a value.
- **HTTP service mode** (`-serve`): a small stdlib-only REST API (`PUT`/`GET`/`DELETE`/`scan`/`stats`/`healthz`) for running the store as a standalone networked service.
- **Observability**: a dependency-free `metrics` package rendering Prometheus text-exposition format, instrumented throughout the engine (request counts, hit/miss rates, latency histograms, per-level SSTable gauges, flush/compaction counts and failures); structured logging via `log/slog` (with a `-log-json` flag for log-aggregation-friendly output); and a separate debug/ops HTTP listener (`-debug-addr`) exposing `/metrics` and `net/http/pprof`, deliberately isolated from the public API port.

## Design decisions worth calling out

**Recency is resolved by per-record sequence number, not table order,
table "version", or compaction level.** Every write is assigned a
globally monotonic `Seq` at the moment it's accepted, and that `Seq` is
carried unchanged through every future compaction. `Get`/`Scan` never
assume anything about slice order or which table is "newest" - they
compare the `Seq` on every candidate record they find. This was essential
for correctness (see the bug list below) and is also exactly what makes
snapshots possible: a snapshot is just a pinned `Seq` value, filtered
against at read time.

**Flushing is single-threaded by design.** A dedicated `flushWorker`
goroutine processes rotated-out MemTables strictly one at a time, in the
order they were rotated - always into level 0. This guarantees flushes
complete in the same order the underlying writes happened, which is what
lets background compaction safely conclude it has seen every table that
could hold an older value for a key before dropping a tombstone.

**Leveled compaction picks whole levels, not partial key ranges.** L0->L1
compaction always takes every L0 table (they may overlap each other)
plus every L1 table; Ln->Ln+1 compaction (n>=1) takes every table in
level n plus every table in level n+1. A real production system (RocksDB,
LevelDB) instead picks specific overlapping files to bound the cost of
any one compaction - the whole-level approach here is a deliberate
simplicity trade-off, still enough to demonstrate genuine multi-level
space/read-amplification control.

**Compaction determines "is it safe to drop a tombstone" via a
conservative, whole-level check**: a compaction pass is only "bottommost"
for its keys if no table exists at any level *deeper* than the one it's
compacting into (rather than a precise per-key-range overlap check
against deeper levels). This trades a little delayed space reclamation
for a much simpler correctness argument.

**SSTable file lifetime is reference-counted.** A reader (a `Get`, a
`Scan`, or compaction's own merge) can be in the middle of reading a
`Table` at the exact moment compaction decides to supersede it.
`Table.Acquire`/`Release`/`Retire` defer the actual file close (and, for
compacted-away tables, deletion) until every outstanding reader is done.

### Snapshot isolation caveat

A `Snapshot` is guaranteed consistent for any key whose value, as of the
snapshot, had already survived at least one MemTable rotation without
being overwritten again in the *same* still-mutable MemTable generation.
In the narrow window where a key is written more than once within one
MemTable's lifetime, the MemTable only retains the latest write (it is a
plain overwrite map, not itself multi-versioned) - so a snapshot taken
between those writes may observe the newer value until that MemTable
flushes or rotates. Once a write has left the active MemTable, full
snapshot isolation applies, because SSTables retain every version an open
snapshot might still need (see `sstable.Merge`'s `minKeepSeq`
parameter and the MVCC-style retention walk it drives).

This is a deliberate, bounded-scope decision rather than an oversight:
true full-history MVCC would require the MemTable itself to be
multi-versioned (as RocksDB's memtable, keyed on `{userkey, seq}`, is),
which is a significantly larger change than fits this project's scope.
The engine and CLI/README are explicit about the boundary rather than
silently claiming stronger guarantees than are actually provided.

### Bugs found and fixed while building this

Every feature here was validated by writing (and then deliberately
stress-running, with `-race`, dozens of iterations of) integration tests
rather than by inspection alone. Several real, non-obvious bugs were
caught this way:

1. **Trusting SSTable list order for recency.** An early `Get()` assumed
   the last-appended table was newest. Two background operations (a
   flush and a compaction) can finish - and get appended - in the
   opposite order they were started. Fixed by resolving recency
   explicitly per-record via `Seq`, never by list position.
2. **Compaction re-numbering ("leapfrogging") data it touched.** Stamping
   each compacted output with a fresh version number could promote a
   long-untouched key above a genuinely newer tombstone that hadn't been
   swept into the same pass yet, silently resurrecting deleted data.
   Fixed by switching entirely to the immutable per-record `Seq`.
3. **A reader racing with compaction's file close/delete.** Fixed with
   the reference-counted `Table` lifecycle described above.
4. **Concurrent flush goroutines completing out of write-order**, letting
   a delete's SSTable be compacted before an earlier put's SSTable for
   the same key had even been written. Fixed by serializing all flushes
   through one worker goroutine.
5. **Dropping a tombstone while an older version was retained for a
   snapshot.** Once snapshot support needed compaction to sometimes keep
   more than one version of a key, an unconditional "drop the tombstone
   at bottommost" rule became unsafe: with the tombstone gone, an
   ordinary unbounded read would fall through to the older,
   snapshot-only version and see it as if it were still live. Fixed by
   only allowed dropping the tombstone when no active snapshot needs
   anything older than it either - i.e. exactly when nothing would be
   left standing below it anyway.

## Usage as a library

```go
import "kvstore/engine"

e, err := engine.Open("./data",
    engine.WithFlushThreshold(4*1024*1024),
    engine.WithL0CompactionTrigger(4),
    engine.WithBaseLevelSizeBytes(2*1024*1024),
    engine.WithLevelSizeMultiplier(10),
    engine.WithCompression(false),
)
if err != nil {
    // handle
}
defer e.Close()

err = e.Put([]byte("key"), []byte("value"))
val, err := e.Get([]byte("key"))     // engine.ErrNotFound if absent/deleted
err = e.Delete([]byte("key"))

// Range scan: nil start/end means unbounded on that side.
it, err := e.Scan([]byte("a"), []byte("m"))
defer it.Close()
for {
    key, value, ok, err := it.Next()
    if err != nil { /* handle */ }
    if !ok { break }
    // use key, value
}

// Point-in-time snapshot.
snap, err := e.Snapshot()
defer snap.Close()
val, err = snap.Get([]byte("key"))            // as of snap's creation time
it, err = snap.Scan(nil, nil)                 // likewise
```

## CLI

```
go run . -dir ./data                              # interactive REPL
go run . -dir ./data -bench                        # concurrent read/write benchmark
go run . -dir ./data -serve -addr :8080            # HTTP server
go run . -dir ./data -serve -debug-addr :6060      # + metrics/pprof on a separate debug port (default on)
go run . -dir ./data -serve -log-json              # structured JSON logs instead of text
```

By default, running with `-serve` also starts a **separate** debug/ops
listener on `:6060` (override with `-debug-addr`, or set it to `""` to
disable) exposing `/metrics` (Prometheus text format) and
`/debug/pprof/*` (`net/http/pprof`). This is deliberately a different
port from the public API (`-addr`): metrics and profiling endpoints
should not be reachable from wherever untrusted clients can reach the KV
API itself, so in a real deployment `-debug-addr` would typically be
bound to localhost or otherwise firewalled off, separately from
whatever's in front of `-addr`.

REPL commands: `PUT <key> <value>`, `GET <key>`, `DELETE <key>`,
`SCAN <start> <end> [limit]` (use `""` for unbounded), `SNAPSHOT` /
`SNAPSHOT-END` (subsequent `GET`/`SCAN` read as of that point in time
until closed), `STATS`, `EXIT`.

### HTTP API (stdlib `net/http` only, no framework)

```
PUT    /kv/{key}                     body = raw value bytes   -> 204
GET    /kv/{key}                                              -> 200 + raw value, 404 if absent
DELETE /kv/{key}                                              -> 204
GET    /scan?start=&end=&limit=                                -> 200 + JSON [{"key","value"} base64]
GET    /stats                                                  -> 200 + JSON engine.Stats
GET    /healthz                                                -> 200 "ok"
```

```
curl -X PUT --data 'hello' http://localhost:8080/kv/greeting
curl http://localhost:8080/kv/greeting
curl "http://localhost:8080/scan?start=a&end=z&limit=100"
curl http://localhost:8080/stats
```

## Running the tests

```
go test ./...                        # correctness
go test ./... -race -count=20        # stress the concurrency guarantees
```

Tests are split across three packages:

- `metrics`: unit tests for the metrics primitives themselves - counter/
  gauge/vec rendering, histogram bucket cumulativeness, and a concurrent
  race test.
- `sstable`: unit tests for the storage primitives in isolation - write/read
  round-trips, multi-version `Get` respecting a `Seq` bound, range
  iteration, compression round-trips, the reference-counted `Table`
  lifecycle, and (critically) `Merge`'s snapshot-aware retention logic,
  including a regression test for bug #5 above.
- `engine`: end-to-end integration tests - basic CRUD, flush/reopen,
  crash recovery (including a deliberately corrupted WAL tail), leveled
  compaction promoting tables across levels, range scans across the
  MemTable and multiple SSTables at once, snapshot isolation, compression
  round-tripping through the full engine, metrics reflecting real
  operations, and concurrent stress tests (puts/deletes/overwrites racing
  across many goroutines against shared keys with tiny flush/compaction
  thresholds) - the regime that exposed every bug listed above.

## Known limitations (by design, for a bounded scope)

- Leveled compaction merges whole levels rather than picking specific
  overlapping files, and its "safe to drop a tombstone" check is a
  conservative whole-level test rather than a precise per-key-range
  overlap check against deeper levels.
- The SSTable index is a full per-record index rather than a sparse one;
  simpler and fine at moderate scale, but uses more memory per table than
  a sparse index would at very large table sizes.
- Per-value compression (rather than per-block) keeps each record's byte
  offset meaningful for direct seeks, at the cost of a worse compression
  ratio than block-level compression would give (no shared dictionary
  across records).
- Snapshot isolation has the in-memory-overwrite caveat described above:
  it is not full MVCC, since the MemTable itself is not multi-versioned.
- If a background flush repeatedly fails (e.g. persistent disk-full), the
  affected MemTable stays queued in memory with no automatic retry loop
  beyond process restart.
