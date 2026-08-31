package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"kvstore/bloom"
	"kvstore/memtable"
)

// Table is an opened, immutable SSTable ready for point lookups and
// iteration. Its file handle is safe for concurrent use: point lookups go
// through os.File.ReadAt, which does not share a mutable seek offset
// across goroutines, so many readers can hit the same Table in parallel
// without any locking for the read path itself.
//
// Lifetime is reference-counted (see Acquire/Release/Retire) because a
// Table can be concurrently in use by a reader (which merely holds a
// pointer obtained from a snapshot of the engine's table list) at the
// exact moment compaction decides to supersede and delete it. Without
// reference counting, closing the file out from under an in-flight
// ReadAt would surface as a "file already closed" error - reference
// counting instead defers the actual close (and, if the table was
// compacted away, the file deletion) until every outstanding user is
// done with it.
type Table struct {
	path        string
	file        *os.File
	index       []indexEntry
	filter      *bloom.Filter
	numEntries  uint64
	minKey      []byte
	maxKey      []byte
	dataBlockSz int64 // length of the data block, i.e. where it ends
	fileSize    int64

	lifecycleMu  sync.Mutex
	refCount     int
	removeOnZero bool
	released     bool
}

// Open opens the SSTable file at path, which must have been produced by
// Write (or Merge), and loads its index and Bloom filter into memory.
func Open(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %q: %w", path, err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %q: %w", path, err)
	}
	if stat.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable: %q is only %d bytes, too small to contain a valid footer (corrupt or truncated)", path, stat.Size())
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, stat.Size()-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: reading footer of %q: %w", path, err)
	}

	magic := binary.LittleEndian.Uint32(footer[40:44])
	if magic != magicNumber {
		f.Close()
		return nil, fmt.Errorf("sstable: %q failed magic-number validation (got 0x%X, want 0x%X): file is corrupt or not an SSTable", path, magic, magicNumber)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
	bloomLen := binary.LittleEndian.Uint64(footer[24:32])
	numEntries := binary.LittleEndian.Uint64(footer[32:40])

	if indexOffset+indexLen > uint64(stat.Size()) || bloomOffset+bloomLen > uint64(stat.Size()) {
		f.Close()
		return nil, fmt.Errorf("sstable: %q footer references offsets beyond end of file (corrupt)", path)
	}

	indexBuf := make([]byte, indexLen)
	if indexLen > 0 {
		if _, err := f.ReadAt(indexBuf, int64(indexOffset)); err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: reading index block of %q: %w", path, err)
		}
	}

	index := make([]indexEntry, 0, numEntries)
	pos := 0
	for pos < len(indexBuf) {
		if pos+4 > len(indexBuf) {
			f.Close()
			return nil, fmt.Errorf("sstable: truncated index entry in %q at byte %d", path, pos)
		}
		klen := binary.LittleEndian.Uint32(indexBuf[pos : pos+4])
		pos += 4
		if pos+int(klen)+8 > len(indexBuf) {
			f.Close()
			return nil, fmt.Errorf("sstable: truncated index entry in %q at byte %d", path, pos)
		}
		key := append([]byte(nil), indexBuf[pos:pos+int(klen)]...)
		pos += int(klen)
		off := binary.LittleEndian.Uint64(indexBuf[pos : pos+8])
		pos += 8
		index = append(index, indexEntry{key: key, offset: off})
	}

	bloomBuf := make([]byte, bloomLen)
	if bloomLen > 0 {
		if _, err := f.ReadAt(bloomBuf, int64(bloomOffset)); err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: reading bloom filter block of %q: %w", path, err)
		}
	}
	filter, err := bloom.Decode(bloomBuf)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: decoding bloom filter of %q: %w", path, err)
	}

	var minKey, maxKey []byte
	if len(index) > 0 {
		minKey = index[0].key
		maxKey = index[len(index)-1].key
	}

	return &Table{
		path:        path,
		file:        f,
		index:       index,
		filter:      filter,
		numEntries:  numEntries,
		minKey:      minKey,
		maxKey:      maxKey,
		dataBlockSz: int64(indexOffset),
		fileSize:    stat.Size(),
		refCount:    1, // the caller (typically the engine's table list) holds the initial reference
	}, nil
}

// Path returns the filesystem path of this table.
func (t *Table) Path() string { return t.path }

// NumEntries returns the number of records stored, including tombstones
// and superseded older versions.
func (t *Table) NumEntries() uint64 { return t.numEntries }

// SizeBytes returns the on-disk file size, used by leveled compaction to
// decide when a level has grown past its target size.
func (t *Table) SizeBytes() int64 { return t.fileSize }

// KeyRange returns the smallest and largest key stored in this table, or
// (nil, nil) if the table is empty.
func (t *Table) KeyRange() (min, max []byte) { return t.minKey, t.maxKey }

// MayContainRange reports whether key could possibly fall within this
// table's key range, as a cheap pre-filter before consulting the Bloom
// filter and index.
func (t *Table) MayContainRange(key []byte) bool {
	if t.minKey == nil {
		return false
	}
	return bytes.Compare(key, t.minKey) >= 0 && bytes.Compare(key, t.maxKey) <= 0
}

// Acquire takes out one more reference on the table, returning false if
// the table has already been fully released (which should not happen for
// a caller that follows the documented protocol of acquiring while still
// holding the engine's read lock on the table list). Every successful
// Acquire must be paired with exactly one Release.
func (t *Table) Acquire() bool {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.released {
		return false
	}
	t.refCount++
	return true
}

// Release gives up one reference. When the reference count reaches zero,
// the underlying file descriptor is closed and, if the table was
// previously marked via Retire, its backing file is deleted from disk.
func (t *Table) Release() error {
	t.lifecycleMu.Lock()
	t.refCount--
	if t.refCount > 0 {
		t.lifecycleMu.Unlock()
		return nil
	}
	remove := t.removeOnZero
	t.released = true
	t.lifecycleMu.Unlock()

	err := t.file.Close()
	if remove {
		if rerr := os.Remove(t.path); rerr != nil && !os.IsNotExist(rerr) && err == nil {
			err = rerr
		}
	}
	if err != nil {
		return fmt.Errorf("sstable: releasing %q: %w", t.path, err)
	}
	return nil
}

// Retire marks the table as superseded (normally because compaction has
// folded its contents into a new table) and gives up the caller's own
// reference. Once every other outstanding reference - held by readers
// that had already acquired this table before it was retired - is also
// released, the file is closed and deleted from disk. This ordering is
// what makes it safe for a reader to still be using a Table at the exact
// moment compaction decides to supersede it.
func (t *Table) Retire() error {
	t.lifecycleMu.Lock()
	t.removeOnZero = true
	t.lifecycleMu.Unlock()
	return t.Release()
}

// Get performs a point lookup for the most recent version of key with
// Seq <= maxSeq (pass ^uint64(0) for "no limit", i.e. the latest version
// unconditionally - what a normal, non-snapshot Get wants). found is
// false if no such version exists in this table.
//
// A table may hold more than one record for the same key (see the
// package doc); within a key's group they are stored newest-first (Seq
// descending), so after the binary search locates the start of the
// group, at most a few sequential reads are needed to find the first
// version satisfying the Seq bound.
func (t *Table) Get(key []byte, maxSeq uint64) (rec Record, found bool, err error) {
	if !t.MayContainRange(key) {
		return Record{}, false, nil
	}
	if !t.filter.MayContain(key) {
		return Record{}, false, nil
	}

	i := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].key, key) >= 0
	})
	for ; i < len(t.index) && bytes.Equal(t.index[i].key, key); i++ {
		rec, err = readRecordAt(t.file, int64(t.index[i].offset))
		if err != nil {
			return Record{}, false, fmt.Errorf("sstable: reading record for key in %q: %w", t.path, err)
		}
		if rec.Seq <= maxSeq {
			return rec, true, nil
		}
	}
	return Record{}, false, nil
}

// readRecordAt decodes a single data-block record at the given file
// offset using ReadAt, which is concurrency-safe across goroutines since
// it does not depend on (or mutate) a shared file cursor. Transparently
// decompresses the value if the record's compressed flag is set.
func readRecordAt(f *os.File, offset int64) (Record, error) {
	header := make([]byte, dataHeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		return Record{}, fmt.Errorf("read header at offset %d: %w", offset, err)
	}
	keyLen := binary.LittleEndian.Uint32(header[0:4])
	seq := binary.LittleEndian.Uint64(header[4:12])
	rawTyp := header[12]
	valLen := binary.LittleEndian.Uint32(header[13:17])

	bodyLen := int(keyLen) + int(valLen)
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := f.ReadAt(body, offset+dataHeaderSize); err != nil {
			return Record{}, fmt.Errorf("read body at offset %d: %w", offset+dataHeaderSize, err)
		}
	}

	value := body[keyLen:]
	if rawTyp&compressedFlag != 0 {
		dv, err := decompressValue(value)
		if err != nil {
			return Record{}, fmt.Errorf("record at offset %d: %w", offset, err)
		}
		value = dv
	}

	return Record{
		Key:   body[:keyLen],
		Value: value,
		Type:  memtable.ValueType(rawTyp &^ compressedFlag),
		Seq:   seq,
	}, nil
}

// Iterator sequentially scans a Table's ENTIRE data block (every version
// of every key) in on-disk order. Used by compaction to merge multiple
// tables together, where every version matters. Each Iterator owns its
// own file handle, independent of the Table's, so iteration can proceed
// concurrently with point lookups against the same Table.
type Iterator struct {
	f     *os.File
	r     *bufio.Reader
	pos   int64
	limit int64
}

// NewIterator opens a fresh sequential scan over the table's data block.
func (t *Table) NewIterator() (*Iterator, error) {
	f, err := os.Open(t.path)
	if err != nil {
		return nil, fmt.Errorf("sstable: opening %q for iteration: %w", t.path, err)
	}
	return &Iterator{f: f, r: bufio.NewReaderSize(f, 64*1024), limit: t.dataBlockSz}, nil
}

// Next advances the iterator, returning the next record (any version) in
// on-disk order, or ok=false once the data block is exhausted.
func (it *Iterator) Next() (rec Record, ok bool, err error) {
	if it.pos >= it.limit {
		return Record{}, false, nil
	}
	header := make([]byte, dataHeaderSize)
	if _, err := io.ReadFull(it.r, header); err != nil {
		return Record{}, false, fmt.Errorf("sstable: iterator header read: %w", err)
	}
	keyLen := binary.LittleEndian.Uint32(header[0:4])
	seq := binary.LittleEndian.Uint64(header[4:12])
	rawTyp := header[12]
	valLen := binary.LittleEndian.Uint32(header[13:17])

	key := make([]byte, keyLen)
	if keyLen > 0 {
		if _, err := io.ReadFull(it.r, key); err != nil {
			return Record{}, false, fmt.Errorf("sstable: iterator key read: %w", err)
		}
	}
	var value []byte
	if valLen > 0 {
		value = make([]byte, valLen)
		if _, err := io.ReadFull(it.r, value); err != nil {
			return Record{}, false, fmt.Errorf("sstable: iterator value read: %w", err)
		}
	}
	if rawTyp&compressedFlag != 0 {
		dv, err := decompressValue(value)
		if err != nil {
			return Record{}, false, fmt.Errorf("sstable: iterator decompress: %w", err)
		}
		value = dv
	}

	it.pos += int64(dataHeaderSize) + int64(keyLen) + int64(valLen)
	return Record{Key: key, Value: value, Type: memtable.ValueType(rawTyp &^ compressedFlag), Seq: seq}, true, nil
}

// Close releases the iterator's independent file handle.
func (it *Iterator) Close() error {
	return it.f.Close()
}

// RangeIterator walks a Table's index over [start, end), returning at
// most one record per key: the newest version with Seq <= maxSeq (older
// versions and versions too new for maxSeq are skipped automatically).
// It uses the in-memory index rather than a streaming file scan, which
// is possible because the index already holds one entry per record in
// sorted order.
type RangeIterator struct {
	t      *Table
	idx    int
	end    []byte
	maxSeq uint64
}

// NewRangeIterator returns a RangeIterator over keys in [start, end)
// (nil start means "from the beginning", nil end means "to the end"),
// exposing only versions with Seq <= maxSeq.
func (t *Table) NewRangeIterator(start, end []byte, maxSeq uint64) *RangeIterator {
	i := 0
	if start != nil {
		i = sort.Search(len(t.index), func(i int) bool {
			return bytes.Compare(t.index[i].key, start) >= 0
		})
	}
	return &RangeIterator{t: t, idx: i, end: end, maxSeq: maxSeq}
}

// Next returns the next visible (key, record) pair in ascending key
// order, skipping any key whose only versions are all newer than maxSeq,
// and skipping every version of a key after the first one satisfying the
// Seq bound (since within a key's group, entries are newest-first).
func (it *RangeIterator) Next() (rec Record, ok bool, err error) {
	for it.idx < len(it.t.index) {
		key := it.t.index[it.idx].key
		if it.end != nil && bytes.Compare(key, it.end) >= 0 {
			return Record{}, false, nil
		}

		// Scan this key's version group (contiguous, newest-first),
		// looking for the first one visible at maxSeq.
		var found *Record
		for it.idx < len(it.t.index) && bytes.Equal(it.t.index[it.idx].key, key) {
			if found == nil {
				r, err := readRecordAt(it.t.file, int64(it.t.index[it.idx].offset))
				if err != nil {
					return Record{}, false, fmt.Errorf("sstable: range iterator: %w", err)
				}
				if r.Seq <= it.maxSeq {
					found = &r
				}
			}
			it.idx++
		}
		if found != nil {
			return *found, true, nil
		}
		// Every version of this key was newer than maxSeq: nothing
		// visible for it, move on to the next key.
	}
	return Record{}, false, nil
}
