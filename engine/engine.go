// Package engine implements the top-level LSM-tree key-value store,
// composing the WAL, MemTable, and SSTable packages into a single
// durable, concurrent storage engine with:
//
//   - Write-ahead logging: every write is fsynced to disk before it is
//     applied to the in-memory state, guaranteeing durability across
//     crashes.
//   - Crash recovery: on Open, any WAL left over from an unclean shutdown
//     is replayed to rebuild lost in-memory state before the store
//     accepts new writes.
//   - A concurrent MemTable that is rotated out and flushed to an
//     immutable, sorted SSTable (always at level 0) once it grows past a
//     configurable size.
//   - A background, leveled compaction worker: L0 (overlapping, one per
//     flush) compacts into L1 once it accumulates too many tables; each
//     level above that compacts into the next once its total size
//     exceeds a target that grows geometrically with level, discarding
//     shadowed keys and - once truly safe to - tombstones.
//   - Range scans and point-in-time snapshot reads, both built on the
//     same per-write monotonic sequence number used for compaction's
//     recency resolution.
package engine

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/memtable"
	"kvstore/metrics"
	"kvstore/sstable"
	"kvstore/wal"
)

// ErrClosed is returned by any operation attempted after Close.
var ErrClosed = errors.New("engine: store is closed")

// ErrNotFound is returned by Get when the key does not exist (or has been
// deleted).
var ErrNotFound = errors.New("engine: key not found")

// ErrEmptyKey is returned when Put, Delete, or Get is called with an
// empty key.
var ErrEmptyKey = errors.New("engine: key must not be empty")

const (
	defaultFlushThreshold      = 4 * 1024 * 1024 // 4 MiB of estimated MemTable data
	defaultL0CompactionTrigger = 4               // number of L0 tables that triggers L0->L1 compaction
	defaultBaseLevelSizeBytes  = 2 * 1024 * 1024 // target size of L1 before L1->L2 triggers
	defaultLevelSizeMultiplier = 10              // each level's target size is this many times the level below it
	defaultMaxLevels           = 7
	compactionQueueDepth       = 1

	// flushQueueDepth bounds how many rotated-out MemTables can be
	// queued for flushing before rotate() starts applying backpressure
	// by blocking. Flushing is deliberately single-threaded (see
	// flushWorker) so that MemTables are always persisted to SSTables in
	// exactly the order they were rotated out - which is the same order
	// their writes originally happened in. This ordering guarantee is
	// what lets compaction safely drop a tombstone once it has been
	// merged together with every table that could hold an older value
	// for that key: an older write for the same key can only ever have
	// been queued for flushing earlier, and therefore is guaranteed to
	// have already been applied to the table list by the time a later
	// (e.g. a delete's) flush completes. Concurrent flushing would break
	// that guarantee, since two flushes could complete in an order that
	// does not match the order their underlying writes occurred in.
	flushQueueDepth = 16
)

// Option configures an Engine at Open time.
type Option func(*Engine)

// WithFlushThreshold sets the approximate MemTable size, in bytes, at
// which it is rotated out and flushed to a new level-0 SSTable.
func WithFlushThreshold(bytes int64) Option {
	return func(e *Engine) { e.flushThreshold = bytes }
}

// WithL0CompactionTrigger sets the number of level-0 SSTables that
// triggers an L0->L1 compaction pass. Level 0 is special: because each
// L0 table comes straight from a MemTable flush, tables within L0 may
// have overlapping key ranges, so an L0 compaction always takes every
// L0 table (plus every table it overlaps in L1) at once.
func WithL0CompactionTrigger(n int) Option {
	return func(e *Engine) { e.l0CompactionTrigger = n }
}

// WithBaseLevelSizeBytes sets the target total size of level 1 before an
// L1->L2 compaction triggers. Each level above that has a target size
// this many times larger than the level below it (see
// WithLevelSizeMultiplier), mirroring how real leveled-compaction LSM
// trees bound space and read amplification as data grows.
func WithBaseLevelSizeBytes(bytes int64) Option {
	return func(e *Engine) { e.baseLevelSizeBytes = bytes }
}

// WithLevelSizeMultiplier sets the growth factor between one level's
// target size and the next.
func WithLevelSizeMultiplier(n int64) Option {
	return func(e *Engine) { e.levelSizeMultiplier = n }
}

// WithMaxLevels sets the number of levels (0..n-1) the compactor will
// use. Level 0 always holds fresh flushes; levels 1..n-1 are compacted
// into as data accumulates.
func WithMaxLevels(n int) Option {
	return func(e *Engine) { e.maxLevels = n }
}

// WithCompression enables per-value DEFLATE compression for newly
// written SSTable data (both fresh flushes and compaction output).
// Existing uncompressed data remains readable regardless of this
// setting - the compressed/uncompressed state is tracked per record, not
// per table or per store.
func WithCompression(enabled bool) Option {
	return func(e *Engine) { e.compress = enabled }
}

// WithLogger sets the structured logger the engine uses for warnings and
// errors from its background workers (flush failures, compaction
// failures, and similar non-fatal issues that would otherwise be
// silently swallowed). Defaults to slog.Default() if not set.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) { e.logger = l }
}

// WithMetricsRegistry supplies an existing metrics.Registry for the
// engine to register its instrumentation on (useful if the embedding
// application wants to combine engine metrics with its own on a single
// /metrics endpoint). If not set, Open creates a fresh, private registry,
// retrievable afterward via Engine.MetricsRegistry.
func WithMetricsRegistry(r *metrics.Registry) Option {
	return func(e *Engine) { e.metricsRegistry = r }
}

// flushJob is a rotated-out MemTable waiting to be persisted to a new
// SSTable, along with the WAL that still backs it until that happens.
type flushJob struct {
	mt      *memtable.MemTable
	walPath string
}

// immEntry pairs an immutable MemTable awaiting flush with the WAL file
// that still backs it (needed for crash recovery until the flush
// succeeds).
type immEntry struct {
	mt      *memtable.MemTable
	walPath string
}

// leveledTable pairs an on-disk SSTable with the compaction level it
// currently lives at.
type leveledTable struct {
	level int
	table *sstable.Table
}

// Engine is a single embedded LSM-tree key-value store rooted at one
// directory. All exported methods are safe for concurrent use.
type Engine struct {
	dir string

	// mu guards mem, wal, walPath, imm, tables, and closed. The hot read
	// path (Get) and the hot write path (Put/Delete) both take the read
	// lock so arbitrarily many of them can proceed in parallel; only
	// rotation (memtable/WAL swap) and compaction bookkeeping take the
	// write lock, and both are comparatively rare.
	mu      sync.RWMutex
	mem     *memtable.MemTable
	wal     *wal.WAL
	walPath string
	imm     []immEntry
	tables  []leveledTable // no ordering invariant across levels; Get()/Scan() resolve recency via each record's Seq
	closed  bool

	seq        uint64 // atomic: next sequence number to assign
	genCounter uint64 // atomic: next file generation number to assign (unique on-disk file id, unrelated to recency)

	flushThreshold      int64
	l0CompactionTrigger int
	baseLevelSizeBytes  int64
	levelSizeMultiplier int64
	maxLevels           int
	compress            bool

	// snapMu guards activeSnapshots, tracking which sequence numbers
	// currently have an open Snapshot pinned to them. Compaction consults
	// the minimum of this set before deciding it's safe to drop an
	// older, superseded version of a key (see sstable.Merge's
	// minKeepSeq parameter).
	snapMu          sync.Mutex
	activeSnapshots map[uint64]int // seq -> number of open snapshots pinned at that seq

	logger          *slog.Logger
	metricsRegistry *metrics.Registry
	m               *engineMetrics

	compactCh chan struct{}
	flushCh   chan flushJob
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// Open opens the store rooted at dir, creating it if necessary, and
// recovers any state left over from an unclean shutdown by replaying
// WAL files found on disk. It then starts the background compaction
// worker and returns a ready-to-use Engine.
func Open(dir string, opts ...Option) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: creating directory %q: %w", dir, err)
	}

	e := &Engine{
		dir:                 dir,
		flushThreshold:      defaultFlushThreshold,
		l0CompactionTrigger: defaultL0CompactionTrigger,
		baseLevelSizeBytes:  defaultBaseLevelSizeBytes,
		levelSizeMultiplier: defaultLevelSizeMultiplier,
		maxLevels:           defaultMaxLevels,
		activeSnapshots:     make(map[uint64]int),
		stopCh:              make(chan struct{}),
		compactCh:           make(chan struct{}, compactionQueueDepth),
		flushCh:             make(chan flushJob, flushQueueDepth),
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.logger == nil {
		e.logger = slog.Default()
	}
	if e.metricsRegistry == nil {
		e.metricsRegistry = metrics.NewRegistry()
	}
	e.m = newEngineMetrics(e.metricsRegistry)

	walFiles, sstFiles, maxGen, err := scanDir(dir)
	if err != nil {
		return nil, err
	}
	e.genCounter = maxGen

	for _, sf := range sstFiles {
		t, err := sstable.Open(sf.path)
		if err != nil {
			return nil, fmt.Errorf("engine: opening existing sstable %q: %w", sf.path, err)
		}
		e.tables = append(e.tables, leveledTable{level: sf.level, table: t})
	}

	if err := e.recoverFromWALs(walFiles); err != nil {
		return nil, err
	}

	newGen := e.nextGen()
	walP := walFileName(dir, newGen)
	w, err := wal.Open(walP)
	if err != nil {
		return nil, fmt.Errorf("engine: opening new wal %q: %w", walP, err)
	}
	e.wal = w
	e.walPath = walP
	e.mem = memtable.New()

	e.wg.Add(1)
	go e.compactionLoop()

	e.wg.Add(1)
	go e.flushWorker()

	return e, nil
}

// recoverFromWALs replays every leftover WAL file (oldest generation
// first) into a fresh MemTable, flushes that MemTable to a new level-0
// SSTable if it is non-empty, and then deletes the now-redundant WAL
// files. This guarantees the store starts every session with at most one
// active WAL, matching its steady-state invariant.
func (e *Engine) recoverFromWALs(walFiles []string) error {
	if len(walFiles) == 0 {
		return nil
	}

	recovered := memtable.New()
	var maxSeq uint64
	for _, path := range walFiles {
		err := wal.Replay(path, func(rec wal.Record) error {
			if rec.Seq > maxSeq {
				maxSeq = rec.Seq
			}
			switch rec.Type {
			case wal.RecordPut:
				recovered.Put(rec.Key, rec.Value, rec.Seq)
			case wal.RecordDelete:
				recovered.Delete(rec.Key, rec.Seq)
			default:
				return fmt.Errorf("unrecognized WAL record type %d", rec.Type)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("engine: recovering from wal %q: %w", path, err)
		}
	}
	if maxSeq > e.seq {
		e.seq = maxSeq
	}

	if recovered.Len() > 0 {
		gen := e.nextGen()
		path := sstFileName(e.dir, 0, gen)
		if err := sstable.Write(path, buildRecords(recovered), e.compress); err != nil {
			return fmt.Errorf("engine: flushing recovered memtable: %w", err)
		}
		t, err := sstable.Open(path)
		if err != nil {
			return fmt.Errorf("engine: reopening freshly-recovered sstable %q: %w", path, err)
		}
		e.tables = append(e.tables, leveledTable{level: 0, table: t})
	}

	for _, path := range walFiles {
		if err := wal.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("engine: removing recovered wal %q: %w", path, err)
		}
	}
	return nil
}

// Put durably writes key -> value. It first appends and fsyncs a WAL
// record, then applies the write to the active MemTable, satisfying the
// durability guarantee that a successful return means the write will
// survive a subsequent crash.
func (e *Engine) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	start := time.Now()
	err := e.write(key, value, wal.RecordPut)
	e.m.writeLatency.Observe(time.Since(start))
	e.m.putTotal.Inc()
	return err
}

// Delete removes key by writing a tombstone, which shadows any existing
// value for key in the MemTable or in older SSTables until compaction
// eventually reclaims the space.
func (e *Engine) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	start := time.Now()
	err := e.write(key, nil, wal.RecordDelete)
	e.m.writeLatency.Observe(time.Since(start))
	e.m.deleteTotal.Inc()
	return err
}

func (e *Engine) write(key, value []byte, typ wal.RecordType) error {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return ErrClosed
	}

	// NOTE on concurrency correctness: we hold the engine's read lock for
	// the WAL append AND the MemTable mutation, not just to snapshot
	// pointers. rotate() takes the exclusive write lock to swap in a new
	// MemTable/WAL pair, so holding RLock here guarantees rotate() cannot
	// move `mt` into the immutable/flush queue - and have a flush
	// goroutine snapshot its contents via MemTable.All() - while this
	// write is still in the middle of landing on it. Multiple concurrent
	// writers can still hold the RLock simultaneously (that's the point
	// of RWMutex), so this does not serialize the write path.
	seq := atomic.AddUint64(&e.seq, 1)
	w := e.wal
	mt := e.mem

	if err := w.Append(wal.Record{Type: typ, Key: key, Value: value, Seq: seq}); err != nil {
		e.mu.RUnlock()
		return fmt.Errorf("engine: wal append failed: %w", err)
	}
	if typ == wal.RecordPut {
		mt.Put(key, value, seq)
	} else {
		mt.Delete(key, seq)
	}
	full := mt.Size() >= e.flushThreshold
	e.mu.RUnlock()

	if full {
		if err := e.rotate(); err != nil {
			return fmt.Errorf("engine: rotating memtable: %w", err)
		}
	}
	return nil
}

// Get looks up the current value for key, or ErrNotFound if it does not
// exist or has been deleted.
func (e *Engine) Get(key []byte) ([]byte, error) {
	start := time.Now()
	v, err := e.getAt(key, unboundedSeq)
	e.recordGet(start, err)
	return v, err
}

// recordGet updates Get-related metrics; shared by Engine.Get and
// Snapshot.Get.
func (e *Engine) recordGet(start time.Time, err error) {
	e.m.getLatency.Observe(time.Since(start))
	e.m.getTotal.Inc()
	switch {
	case err == nil:
		e.m.getHitTotal.Inc()
	case errors.Is(err, ErrNotFound):
		e.m.getMissTotal.Inc()
	}
}

// unboundedSeq is passed as the maxSeq bound for reads that should see
// every write up to the present moment (i.e. everything except a pinned
// Snapshot).
const unboundedSeq = ^uint64(0)

// getAt is the shared implementation behind Get and Snapshot.Get: it
// looks up key considering only writes with Seq <= maxSeq.
func (e *Engine) getAt(key []byte, maxSeq uint64) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	mt := e.mem
	imm := make([]immEntry, len(e.imm))
	copy(imm, e.imm)
	tables := e.allTablesLocked()
	// Acquire a reference on every table while still holding the engine's
	// read lock. This is what makes it safe to read from these tables
	// after releasing the lock below: compaction can only retire a table
	// while holding the exclusive write lock, so no table present in
	// this snapshot can be retired concurrently with the Acquire calls
	// themselves. Every acquired table is released again below,
	// regardless of whether it held the key we were looking for.
	for _, t := range tables {
		t.Acquire()
	}
	e.mu.RUnlock()
	defer releaseAll(e.logger, tables)

	var (
		best    sstable.Record
		bestSeq uint64
		found   bool
	)
	consider := func(rec sstable.Record) {
		if rec.Seq > maxSeq {
			return // invisible to this read (only relevant for mem/imm, which carry no built-in Seq filtering)
		}
		if !found || rec.Seq > bestSeq {
			best, bestSeq, found = rec, rec.Seq, true
		}
	}

	if entry, ok := mt.Get(key); ok {
		consider(sstable.Record{Value: entry.Value, Type: entry.Type, Seq: entry.Seq})
	}
	for i := len(imm) - 1; i >= 0; i-- {
		if entry, ok := imm[i].mt.Get(key); ok {
			consider(sstable.Record{Value: entry.Value, Type: entry.Type, Seq: entry.Seq})
		}
	}
	// Each table's Get is itself multi-version-aware: it already returns
	// the newest version at or below maxSeq that IT holds, so we only
	// need to compare those per-table winners against each other here.
	for _, t := range tables {
		rec, ok, err := t.Get(key, maxSeq)
		if err != nil {
			return nil, fmt.Errorf("engine: reading sstable %q: %w", t.Path(), err)
		}
		if ok {
			consider(rec)
		}
	}

	if !found {
		return nil, ErrNotFound
	}
	if best.Type == memtable.TypeTombstone {
		return nil, ErrNotFound
	}
	return best.Value, nil
}

func releaseAll(logger *slog.Logger, tables []*sstable.Table) {
	for _, t := range tables {
		if err := t.Release(); err != nil {
			logger.Warn("releasing sstable", "path", t.Path(), "error", err)
		}
	}
}

// allTablesLocked flattens the per-level table list into a flat slice.
// Callers must hold e.mu (for reading or writing).
func (e *Engine) allTablesLocked() []*sstable.Table {
	out := make([]*sstable.Table, len(e.tables))
	for i, lt := range e.tables {
		out[i] = lt.table
	}
	return out
}

// levelTablesLocked returns every table currently at the given level.
// Callers must hold e.mu.
func (e *Engine) levelTablesLocked(level int) []*sstable.Table {
	var out []*sstable.Table
	for _, lt := range e.tables {
		if lt.level == level {
			out = append(out, lt.table)
		}
	}
	return out
}

// rotate swaps in a fresh MemTable and WAL, moves the old MemTable onto
// the immutable/flush queue, and hands it off to the single flush worker
// goroutine (see flushWorker for why flushing is serialized). It
// re-checks the size threshold after acquiring the exclusive lock so
// that if several concurrent writers all observed "full" at once, only
// the first actually rotates.
func (e *Engine) rotate() error {
	e.mu.Lock()

	if e.mem.Size() < e.flushThreshold {
		e.mu.Unlock()
		return nil // another writer already rotated
	}

	oldMem := e.mem
	oldWAL := e.wal
	oldWALPath := e.walPath

	newGen := e.nextGen()
	newWALPath := walFileName(e.dir, newGen)
	newWAL, err := wal.Open(newWALPath)
	if err != nil {
		e.mu.Unlock()
		return fmt.Errorf("opening new wal %q: %w", newWALPath, err)
	}

	e.mem = memtable.New()
	e.wal = newWAL
	e.walPath = newWALPath
	e.imm = append(e.imm, immEntry{mt: oldMem, walPath: oldWALPath})
	e.mu.Unlock()

	// No writer can still be targeting oldMem/oldWAL past this point: any
	// write that had already captured those pointers finished its
	// WAL-append-then-MemTable-write sequence before we acquired the
	// exclusive lock above (see the note in write()). It is therefore
	// safe to close the old WAL's file descriptor now; its data is
	// already durably on disk from each Append's fsync.
	if err := oldWAL.Close(); err != nil {
		e.logger.Warn("closing rotated-out wal", "path", oldWALPath, "error", err)
	}

	// Hand off to the flush worker. This is a plain channel send done
	// without holding e.mu, so a full queue (flushQueueDepth) applies
	// backpressure to writers - rotate() blocks until the worker has
	// drained space - rather than letting an unbounded number of
	// immutable MemTables pile up in memory.
	e.flushCh <- flushJob{mt: oldMem, walPath: oldWALPath}
	return nil
}

// flushWorker is the single background goroutine responsible for
// persisting rotated-out MemTables to level-0 SSTables. It is
// deliberately the only writer of new (non-compaction) SSTables, and
// processes flushJobs strictly one at a time in the order rotate()
// enqueued them - which is the same order the underlying writes
// originally happened in. That ordering is what lets compaction safely
// drop a tombstone once merged with every table that could hold an older
// value for the same key: had flushes run concurrently, a later write's
// (e.g. a delete's) flush could complete and be picked up by compaction
// before an earlier write's flush for the same key had even been
// applied, letting compaction wrongly conclude - from an incomplete view
// - that dropping the tombstone was safe.
func (e *Engine) flushWorker() {
	defer e.wg.Done()
	for {
		select {
		case job := <-e.flushCh:
			e.doFlush(job.mt, job.walPath)
		case <-e.stopCh:
			// Drain whatever was already queued before exiting, so a
			// rotation that had already been handed off is not left
			// unflushed any longer than necessary (it remains safe even
			// if we didn't: its WAL is only removed once flushed, so a
			// crash or an unflushed leftover is simply recovered on the
			// next Open).
			for {
				select {
				case job := <-e.flushCh:
					e.doFlush(job.mt, job.walPath)
				default:
					return
				}
			}
		}
	}
}

// doFlush persists mt to a new level-0 SSTable, registers it, and then
// deletes the WAL that was backing mt (it is now redundant: mt's data is
// durably captured in the new SSTable). If the flush itself fails - e.g.
// disk full - the MemTable and its WAL are left exactly as they were:
// still queryable via Get, and still replayable on the next restart, so
// no data is lost even though this attempt did not succeed.
func (e *Engine) doFlush(mt *memtable.MemTable, walPath string) {
	start := time.Now()
	failed := false
	defer func() {
		e.m.flushTotal.Inc()
		if failed {
			e.m.flushFailedTotal.Inc()
		}
		e.m.flushLatency.Observe(time.Since(start))
	}()

	gen := e.nextGen()
	path := sstFileName(e.dir, 0, gen)
	records := buildRecords(mt)

	if err := sstable.Write(path, records, e.compress); err != nil {
		e.logger.Error("flush failed, WAL retained for retry on restart", "generation", gen, "wal_path", walPath, "error", err)
		failed = true
		return
	}
	table, err := sstable.Open(path)
	if err != nil {
		e.logger.Error("failed to reopen freshly-flushed sstable", "path", path, "error", err)
		failed = true
		return
	}

	e.mu.Lock()
	e.tables = append(e.tables, leveledTable{level: 0, table: table})
	for i, entry := range e.imm {
		if entry.mt == mt {
			e.imm = append(e.imm[:i], e.imm[i+1:]...)
			break
		}
	}
	e.mu.Unlock()

	if err := wal.Remove(walPath); err != nil && !os.IsNotExist(err) {
		e.logger.Warn("failed to remove flushed wal", "path", walPath, "error", err)
	}

	e.maybeTriggerCompaction()
}

// compactionLoop is the background compaction worker: it waits for a
// compaction to be requested (via compactCh) and runs it, until the
// engine is closed.
func (e *Engine) compactionLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.stopCh:
			return
		case <-e.compactCh:
			if err := e.runCompaction(); err != nil {
				e.logger.Error("compaction error", "error", err)
			}
		}
	}
}

// compactionPlan describes one compaction pass: merge inputs (drawn from
// one or two adjacent levels) into targetLevel.
type compactionPlan struct {
	inputs      []*sstable.Table
	targetLevel int
}

// pickCompactionLocked decides what (if anything) to compact next.
// Level 0 is checked first (it bounds read amplification the most,
// since L0 tables may overlap each other); if L0 doesn't need
// compacting, each level above it is checked in order for having grown
// past its target size. Callers must hold e.mu (read lock is enough).
func (e *Engine) pickCompactionLocked() *compactionPlan {
	l0 := e.levelTablesLocked(0)
	if len(l0) >= e.l0CompactionTrigger {
		inputs := append(append([]*sstable.Table{}, l0...), e.levelTablesLocked(1)...)
		return &compactionPlan{inputs: inputs, targetLevel: 1}
	}

	for lvl := 1; lvl < e.maxLevels-1; lvl++ {
		levelTables := e.levelTablesLocked(lvl)
		if len(levelTables) == 0 {
			continue
		}
		var total int64
		for _, t := range levelTables {
			total += t.SizeBytes()
		}
		if total > e.targetSizeForLevel(lvl) {
			inputs := append(append([]*sstable.Table{}, levelTables...), e.levelTablesLocked(lvl+1)...)
			return &compactionPlan{inputs: inputs, targetLevel: lvl + 1}
		}
	}
	return nil
}

// targetSizeForLevel returns the total byte size at which level lvl
// (>=1) should be compacted into the level above it: baseLevelSizeBytes
// for level 1, multiplied by levelSizeMultiplier for each level beyond
// that - the standard geometric growth that keeps each level roughly an
// order of magnitude larger than the one before it.
func (e *Engine) targetSizeForLevel(lvl int) int64 {
	size := e.baseLevelSizeBytes
	for i := 1; i < lvl; i++ {
		size *= e.levelSizeMultiplier
	}
	return size
}

// maybeTriggerCompaction signals the compaction worker if there is
// currently a compaction worth running. Safe to call frequently; the
// buffered, capacity-1 compactCh coalesces redundant signals.
func (e *Engine) maybeTriggerCompaction() {
	e.mu.RLock()
	plan := e.pickCompactionLocked()
	e.mu.RUnlock()
	if plan == nil {
		return
	}
	select {
	case e.compactCh <- struct{}{}:
	default:
	}
}

// minActiveSnapshotSeq returns the smallest Seq currently pinned by an
// open Snapshot, or unboundedSeq if there are none - in which case
// compaction is free to drop any superseded version, since nothing is
// watching for it.
func (e *Engine) minActiveSnapshotSeq() uint64 {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	min := unboundedSeq
	for seq := range e.activeSnapshots {
		if seq < min {
			min = seq
		}
	}
	return min
}

// runCompaction executes one compaction pass, if pickCompactionLocked
// finds one worth running: it merges the plan's input tables into a
// single new table at the target level, discarding shadowed keys and -
// once truly safe to (see isBottommost) - tombstones, then atomically
// swaps the inputs out for the new output table. It is safe to run
// concurrently with new flushes that start after this pass's input
// snapshot is taken: any SSTable that appears later is simply left
// alone and considered in a subsequent pass.
func (e *Engine) runCompaction() (err error) {
	e.mu.RLock()
	plan := e.pickCompactionLocked()
	if plan == nil {
		e.mu.RUnlock()
		return nil
	}
	inputs := plan.inputs
	targetLevel := plan.targetLevel

	// This pass is "bottommost" for its key range - and so free to fully
	// drop a tombstone rather than just carrying it forward - only if no
	// table exists at any level deeper than the one we're compacting
	// into. This is a conservative, whole-level check (rather than a
	// precise per-key-range overlap check against deeper levels), which
	// trades a little delayed space reclamation for a much simpler,
	// easier-to-verify correctness argument.
	isBottommost := true
	for lvl := targetLevel + 1; lvl < e.maxLevels; lvl++ {
		if len(e.levelTablesLocked(lvl)) > 0 {
			isBottommost = false
			break
		}
	}
	e.mu.RUnlock()

	start := time.Now()
	defer func() {
		e.m.compactionTotal.Inc()
		if err != nil {
			e.m.compactionFailedTotal.Inc()
		}
		e.m.compactionLatency.Observe(time.Since(start))
	}()

	minKeepSeq := e.minActiveSnapshotSeq()

	gen := e.nextGen() // uniquely names the output file on disk; unrelated to recency
	outPath := sstFileName(e.dir, targetLevel, gen)

	mergeErr := sstable.Merge(inputs, outPath, minKeepSeq, isBottommost, e.compress)

	e.mu.Lock()
	inSnapshot := make(map[*sstable.Table]bool, len(inputs))
	for _, t := range inputs {
		inSnapshot[t] = true
	}
	remaining := e.tables[:0]
	for _, lt := range e.tables {
		if !inSnapshot[lt.table] {
			remaining = append(remaining, lt)
		}
	}
	if mergeErr == nil {
		t, oerr := sstable.Open(outPath)
		if oerr != nil {
			e.mu.Unlock()
			err = fmt.Errorf("engine: reopening compacted sstable %q: %w", outPath, oerr)
			return err
		}
		remaining = append(remaining, leveledTable{level: targetLevel, table: t})
	} else if !errors.Is(mergeErr, sstable.ErrEmptyMerge) {
		e.mu.Unlock()
		err = fmt.Errorf("engine: compaction merge failed: %w", mergeErr)
		return err
	}
	e.tables = remaining
	e.mu.Unlock()

	// Retire (rather than directly close+remove) each superseded table:
	// this drops the engine's own reference and defers the actual file
	// close/delete until any reader that had concurrently acquired one
	// of these tables (via a Get/Scan snapshot taken just before this
	// compaction committed) finishes with it. See the sstable package
	// doc on Table for why this matters.
	for _, t := range inputs {
		if err := t.Retire(); err != nil {
			e.logger.Warn("retiring compacted-away sstable", "path", t.Path(), "error", err)
		}
	}

	// This pass may have pushed the target level over its own
	// threshold, or L0 may have refilled while we were compacting;
	// re-check and chain another pass if so.
	e.maybeTriggerCompaction()
	return nil
}

// Snapshot pins a consistent, point-in-time view of the store: Get and
// Scan called on it only see writes that had completed at the moment the
// snapshot was taken, regardless of writes that land afterward. Call
// Close when done with it so the engine can reclaim storage for
// superseded versions it was keeping around on the snapshot's behalf.
//
// Isolation caveat: a Snapshot is guaranteed consistent for any key whose
// value, as of the snapshot, had already been through at least one WAL
// append without being overwritten again in the SAME still-unflushed
// MemTable generation. In the narrow window where a key is written more
// than once within one MemTable's lifetime, the MemTable only retains
// the latest write (it is a plain overwrite map, not itself
// multi-versioned) - so a snapshot taken between those writes may
// observe the newer value until that MemTable flushes. Once flushed,
// full snapshot isolation applies, since SSTables retain every version
// an open snapshot might still need (see sstable.Merge's minKeepSeq).
type Snapshot struct {
	e      *Engine
	seq    uint64
	closed int32
}

// Snapshot takes a new point-in-time snapshot pinned at the current
// sequence number.
func (e *Engine) Snapshot() (*Snapshot, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	seq := atomic.LoadUint64(&e.seq)
	e.mu.RUnlock()

	e.snapMu.Lock()
	e.activeSnapshots[seq]++
	e.snapMu.Unlock()
	e.m.snapshotOpenTotal.Inc()
	e.m.activeSnapshots.Add(1)

	return &Snapshot{e: e, seq: seq}, nil
}

// Get looks up key as of the snapshot's point in time.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	start := time.Now()
	v, err := s.e.getAt(key, s.seq)
	s.e.recordGet(start, err)
	return v, err
}

// Scan returns an iterator over [start, end) as of the snapshot's point
// in time. See Engine.Scan for the semantics of start/end and the
// returned iterator.
func (s *Snapshot) Scan(start, end []byte) (*ScanIterator, error) {
	s.e.m.scanTotal.Inc()
	return s.e.scanAt(start, end, s.seq)
}

// Close releases the snapshot. After Close, compaction is once again
// free to reclaim any versions that were being kept only for this
// snapshot's benefit. Close is idempotent.
func (s *Snapshot) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	s.e.snapMu.Lock()
	s.e.activeSnapshots[s.seq]--
	if s.e.activeSnapshots[s.seq] <= 0 {
		delete(s.e.activeSnapshots, s.seq)
	}
	s.e.snapMu.Unlock()
	s.e.m.activeSnapshots.Add(-1)
	return nil
}

// scanSource is a sorted stream of (key, record) pairs, already filtered
// to whatever Seq bound and key range the caller asked for. It underlies
// the heap-merge that ScanIterator performs across the MemTable, every
// immutable MemTable, and every SSTable.
type scanSource interface {
	next() (key []byte, rec sstable.Record, ok bool, err error)
}

// memSliceSource adapts an already-materialized, sorted slice of MemTable
// entries (from MemTable.Range) into a scanSource, filtering out any
// entry newer than maxSeq. A MemTable holds only one version per key, so
// if that version is too new for this read, there is nothing older
// available from this source for that key - it is simply skipped.
type memSliceSource struct {
	entries []memtable.RangeEntry
	idx     int
	maxSeq  uint64
}

func (s *memSliceSource) next() (key []byte, rec sstable.Record, ok bool, err error) {
	for s.idx < len(s.entries) {
		kv := s.entries[s.idx]
		s.idx++
		if kv.Value.Seq <= s.maxSeq {
			return kv.Key, sstable.Record{Value: kv.Value.Value, Type: kv.Value.Type, Seq: kv.Value.Seq}, true, nil
		}
	}
	return nil, sstable.Record{}, false, nil
}

// sstableRangeSource adapts a sstable.RangeIterator (already maxSeq- and
// range-bounded, and already resolved to at most one version per key)
// into a scanSource.
type sstableRangeSource struct {
	ri *sstable.RangeIterator
}

func (s *sstableRangeSource) next() (key []byte, rec sstable.Record, ok bool, err error) {
	rec, ok, err = s.ri.Next()
	if !ok || err != nil {
		return nil, sstable.Record{}, ok, err
	}
	return rec.Key, rec, true, nil
}

type scanHeapItem struct {
	key []byte
	rec sstable.Record
	src scanSource
}

type scanHeap []*scanHeapItem

func (h scanHeap) Len() int { return len(h) }
func (h scanHeap) Less(i, j int) bool {
	c := bytes.Compare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0
	}
	return h[i].rec.Seq > h[j].rec.Seq
}
func (h scanHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *scanHeap) Push(x any)   { *h = append(*h, x.(*scanHeapItem)) }
func (h *scanHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// ScanIterator streams the results of a range scan in ascending key
// order, resolving each key to its single most recent visible version
// across the MemTable, immutable MemTables, and every SSTable, skipping
// deleted keys. Callers MUST call Close when done (whether or not Next
// was iterated to exhaustion) to release the SSTable references the
// iterator holds.
type ScanIterator struct {
	h      *scanHeap
	tables []*sstable.Table
	logger *slog.Logger
	closed bool
}

// Next advances the iterator. ok is false once the scan is exhausted;
// callers should stop calling Next at that point (calling it again is
// safe and simply returns ok=false again).
func (it *ScanIterator) Next() (key, value []byte, ok bool, err error) {
	for it.h.Len() > 0 {
		top := heap.Pop(it.h).(*scanHeapItem)
		key = top.key
		best := top.rec

		if k, r, ok, err := top.src.next(); err != nil {
			return nil, nil, false, err
		} else if ok {
			heap.Push(it.h, &scanHeapItem{key: k, rec: r, src: top.src})
		}
		for it.h.Len() > 0 && bytes.Equal((*it.h)[0].key, key) {
			dup := heap.Pop(it.h).(*scanHeapItem)
			if dup.rec.Seq > best.Seq {
				best = dup.rec
			}
			if k, r, ok, err := dup.src.next(); err != nil {
				return nil, nil, false, err
			} else if ok {
				heap.Push(it.h, &scanHeapItem{key: k, rec: r, src: dup.src})
			}
		}

		if best.Type == memtable.TypeTombstone {
			continue // deleted as of this read; move on to the next key
		}
		return key, best.Value, true, nil
	}
	return nil, nil, false, nil
}

// Close releases every SSTable reference the iterator acquired. Safe to
// call more than once.
func (it *ScanIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	releaseAll(it.logger, it.tables)
	return nil
}

// Scan returns an iterator over every live key in [start, end) (nil
// start means "from the beginning", nil end means "to the end"), as of
// right now.
func (e *Engine) Scan(start, end []byte) (*ScanIterator, error) {
	e.m.scanTotal.Inc()
	return e.scanAt(start, end, unboundedSeq)
}

// scanAt is the shared implementation behind Scan and Snapshot.Scan.
func (e *Engine) scanAt(start, end []byte, maxSeq uint64) (*ScanIterator, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	mt := e.mem
	imm := make([]immEntry, len(e.imm))
	copy(imm, e.imm)
	tables := e.allTablesLocked()
	for _, t := range tables {
		t.Acquire()
	}
	e.mu.RUnlock()

	h := &scanHeap{}
	heap.Init(h)

	pushSource := func(src scanSource) error {
		k, r, ok, err := src.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, &scanHeapItem{key: k, rec: r, src: src})
		}
		return nil
	}

	fail := func(err error) (*ScanIterator, error) {
		releaseAll(e.logger, tables)
		return nil, err
	}

	if err := pushSource(&memSliceSource{entries: mt.Range(start, end), maxSeq: maxSeq}); err != nil {
		return fail(fmt.Errorf("engine: scanning memtable: %w", err))
	}
	for _, ent := range imm {
		if err := pushSource(&memSliceSource{entries: ent.mt.Range(start, end), maxSeq: maxSeq}); err != nil {
			return fail(fmt.Errorf("engine: scanning immutable memtable: %w", err))
		}
	}
	for _, t := range tables {
		ri := t.NewRangeIterator(start, end, maxSeq)
		if err := pushSource(&sstableRangeSource{ri: ri}); err != nil {
			return fail(fmt.Errorf("engine: scanning sstable %q: %w", t.Path(), err))
		}
	}

	return &ScanIterator{h: h, tables: tables, logger: e.logger}, nil
}

// Close stops the background compaction worker, waits for any in-flight
// flush to finish, synchronously flushes whatever remains in the active
// MemTable, and closes every open file descriptor. After Close returns,
// all data is durably represented as SSTables on disk with no leftover
// WAL, and the Engine must not be used again.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.stopCh)
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.mem.Len() > 0 {
		gen := e.nextGen()
		path := sstFileName(e.dir, 0, gen)
		if err := sstable.Write(path, buildRecords(e.mem), e.compress); err != nil {
			return fmt.Errorf("engine: final flush on close failed: %w", err)
		}
		t, err := sstable.Open(path)
		if err != nil {
			return fmt.Errorf("engine: reopening final flushed sstable %q: %w", path, err)
		}
		e.tables = append(e.tables, leveledTable{level: 0, table: t})
	}

	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("engine: closing wal: %w", err)
	}
	if err := wal.Remove(e.walPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("engine: removing final wal: %w", err)
	}

	var firstErr error
	for _, lt := range e.tables {
		// Release (not Retire): a normal Close must not delete any
		// SSTable data, only close file descriptors. If a concurrent
		// caller still holds an acquired reference from an in-flight
		// Get/Scan, the close is safely deferred until they release it
		// (see the sstable package's Table lifecycle documentation).
		if err := lt.table.Release(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("engine: releasing sstable %q: %w", lt.table.Path(), err)
		}
	}
	return firstErr
}

// Stats reports basic runtime counts, useful for monitoring or a
// demo/CLI's status output.
type Stats struct {
	MemTableEntries    int
	ImmutableMemTables int
	SSTables           int
	TablesPerLevel     []int
}

// Stats returns a point-in-time snapshot of engine internals.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	perLevel := make([]int, e.maxLevels)
	for _, lt := range e.tables {
		if lt.level >= 0 && lt.level < len(perLevel) {
			perLevel[lt.level]++
		}
	}

	// Refresh the corresponding gauges as a side effect: this is the
	// only place these values are computed, and it happens under the
	// same read lock, so piggybacking here avoids scattering gauge
	// updates across every mutation site. A Prometheus scrape (or
	// anything else rendering the registry) triggers a Stats() call
	// first via the /metrics handler, so the gauges are always current
	// as of query time.
	e.m.memtableEntries.Set(int64(e.mem.Len()))
	e.m.immutableMemtables.Set(int64(len(e.imm)))
	e.m.sstablesTotal.Set(int64(len(e.tables)))
	for lvl, count := range perLevel {
		e.m.sstablesPerLevel.Set(strconv.Itoa(lvl), int64(count))
	}

	return Stats{
		MemTableEntries:    e.mem.Len(),
		ImmutableMemTables: len(e.imm),
		SSTables:           len(e.tables),
		TablesPerLevel:     perLevel,
	}
}

// MetricsRegistry returns the engine's metrics registry, which a caller
// can render at an HTTP /metrics endpoint (see the server package) or
// combine with its own application metrics.
func (e *Engine) MetricsRegistry() *metrics.Registry {
	return e.metricsRegistry
}

func (e *Engine) nextGen() uint64 {
	return atomic.AddUint64(&e.genCounter, 1)
}

func buildRecords(mt *memtable.MemTable) []sstable.Record {
	kvs := mt.All()
	records := make([]sstable.Record, len(kvs))
	for i, kv := range kvs {
		records[i] = sstable.Record{
			Key:   kv.Key,
			Value: kv.Value.Value,
			Type:  kv.Value.Type,
			Seq:   kv.Value.Seq,
		}
	}
	return records
}

func walFileName(dir string, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.wal", gen))
}

// sstFileName encodes the level directly in the filename
// ("L{level:02d}-{gen:020d}.sst"), so a simple directory listing (see
// scanDir) recovers each table's level without needing to open the file.
func sstFileName(dir string, level int, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("L%02d-%020d.sst", level, gen))
}

type sstFileInfo struct {
	path  string
	level int
	gen   uint64
}

// scanDir lists the data directory for existing WAL and SSTable files
// and returns the highest generation number seen across both, so the
// engine can resume its generation counter without risking a collision
// with an existing file.
func scanDir(dir string) (walFiles []string, sstFiles []sstFileInfo, maxGen uint64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("engine: reading directory %q: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		full := filepath.Join(dir, name)

		if strings.HasSuffix(name, ".wal") {
			base := strings.TrimSuffix(name, ".wal")
			gen, perr := strconv.ParseUint(base, 10, 64)
			if perr != nil {
				continue
			}
			if gen > maxGen {
				maxGen = gen
			}
			walFiles = append(walFiles, full)
			continue
		}

		if strings.HasSuffix(name, ".sst") {
			level, gen, ok := parseSSTFilename(name)
			if !ok {
				continue
			}
			if gen > maxGen {
				maxGen = gen
			}
			sstFiles = append(sstFiles, sstFileInfo{path: full, level: level, gen: gen})
			continue
		}
	}
	sort.Strings(walFiles)
	sort.Slice(sstFiles, func(i, j int) bool { return sstFiles[i].gen < sstFiles[j].gen })
	return walFiles, sstFiles, maxGen, nil
}

// parseSSTFilename parses the "L{level:02d}-{gen:020d}.sst" filename
// format produced by sstFileName.
func parseSSTFilename(name string) (level int, gen uint64, ok bool) {
	base := strings.TrimSuffix(name, ".sst")
	parts := strings.SplitN(base, "-", 2)
	if len(parts) != 2 || len(parts[0]) < 2 || parts[0][0] != 'L' {
		return 0, 0, false
	}
	lvl, err := strconv.Atoi(parts[0][1:])
	if err != nil {
		return 0, 0, false
	}
	g, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return lvl, g, true
}
