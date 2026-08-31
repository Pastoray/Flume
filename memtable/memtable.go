// Package memtable implements the mutable, in-memory write buffer of the
// LSM-tree. All writes land here (after being durably logged to the WAL)
// before eventually being flushed, as an immutable sorted run, to an
// on-disk SSTable.
package memtable

import (
	"sync/atomic"

	"kvstore/skiplist"
)

// ValueType distinguishes a live value from a tombstone (deletion marker).
// Tombstones must be carried through the system - including into SSTables
// and across compactions - so that a Delete correctly shadows older values
// for the same key that may still exist on disk.
type ValueType byte

const (
	TypeValue     ValueType = 1
	TypeTombstone ValueType = 2
)

// Entry is the payload stored per key: a typed value plus the sequence
// number assigned when the write was accepted. The sequence number lets
// the engine deterministically resolve which of several candidate values
// for the same key (across the active MemTable, immutable MemTables, and
// multiple SSTables) is the most recent.
type Entry struct {
	Type  ValueType
	Value []byte
	Seq   uint64
}

// entryOverhead is a rough per-entry bookkeeping cost (skip-list node
// pointers, struct headers, etc.) added to the approximate size tracked
// for flush-threshold decisions. It does not need to be exact - only
// consistent enough to trigger flushes at a sane, bounded MemTable size.
const entryOverhead = 48

// MemTable is a concurrency-safe, sorted, in-memory map of key -> Entry.
type MemTable struct {
	skl  *skiplist.SkipList[Entry]
	size int64 // approximate byte size; read/written atomically
}

// New returns an empty MemTable.
func New() *MemTable {
	return &MemTable{skl: skiplist.New[Entry]()}
}

// Put records a live value for key at sequence number seq.
func (m *MemTable) Put(key, value []byte, seq uint64) {
	v := append([]byte(nil), value...)
	m.skl.Put(key, Entry{Type: TypeValue, Value: v, Seq: seq})
	atomic.AddInt64(&m.size, int64(len(key)+len(v)+entryOverhead))
}

// Delete records a tombstone for key at sequence number seq.
func (m *MemTable) Delete(key []byte, seq uint64) {
	m.skl.Put(key, Entry{Type: TypeTombstone, Value: nil, Seq: seq})
	atomic.AddInt64(&m.size, int64(len(key)+entryOverhead))
}

// Get returns the entry for key, if present. The caller must check
// entry.Type to distinguish a live value from a tombstone.
func (m *MemTable) Get(key []byte) (Entry, bool) {
	return m.skl.Get(key)
}

// Size returns the approximate number of bytes consumed by writes applied
// so far. It is used to decide when the MemTable should be rotated out and
// flushed to disk.
func (m *MemTable) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

// Len returns the number of distinct keys currently held.
func (m *MemTable) Len() int {
	return m.skl.Len()
}

// All returns every key/entry pair in ascending key order, for flushing
// into an SSTable.
func (m *MemTable) All() []RangeEntry {
	return m.skl.All()
}

// Range returns every key/entry pair with key >= start (or from the
// beginning, if start is nil) and key < end (or to the end, if end is
// nil), in ascending key order, for serving a range scan.
func (m *MemTable) Range(start, end []byte) []RangeEntry {
	return m.skl.Range(start, end)
}

// RangeEntry pairs a key with its MemTable entry, as returned by All and
// Range. Exposed as an alias so callers outside this package (namely the
// engine) don't need to import the skiplist package just to name the
// type of a slice returned here.
type RangeEntry = skiplist.KV[Entry]
