// Package sstable implements the Sorted String Table: the immutable,
// on-disk, sorted run produced when a MemTable is flushed, and later
// merged together by compaction.
//
// File layout (all integers little-endian):
//
//	+-------------------------------------------------+
//	| Data Block                                       |
//	|   repeated: [4 keyLen][8 seq][1 type][4 valLen]   |
//	|             [keyLen bytes key][valLen bytes value]|
//	+-------------------------------------------------+
//	| Index Block                                       |
//	|   repeated: [4 keyLen][keyLen bytes key][8 offset] |
//	+-------------------------------------------------+
//	| Bloom Filter Block                                 |
//	|   [4 numBits][4 numHash][bit array]                |
//	+-------------------------------------------------+
//	| Footer (fixed 44 bytes)                           |
//	|   [8 indexOffset][8 indexLen]                      |
//	|   [8 bloomOffset][8 bloomLen]                      |
//	|   [8 numEntries][4 magic]                          |
//	+-------------------------------------------------+
//
// The index holds one entry per RECORD (a "full" index rather than a
// sparse one) which keeps point lookups a simple binary search plus a
// single seek; this trades a little extra index size for simplicity and
// correctness, which is the right call at the data volumes this project
// targets.
//
// Cross-table recency (which record holds the authoritative value for a
// key) is resolved using each RECORD's own Seq number, not any notion of
// "table version". Seq is assigned once, monotonically, at the moment a
// key is written, and is carried forward unchanged through every future
// compaction - so it remains a correct, stable recency marker no matter
// how many times the record is rewritten into a new physical file.
//
// Multiple versions per key: unlike an ordinary point-lookup KV format,
// a table MAY contain more than one record for the same key, sorted
// (within that key's group) by Seq descending - i.e. newest first. This
// is what makes snapshot reads possible: compaction is only allowed to
// drop an old version of a key once no open snapshot could still need
// it (see the engine package's Snapshot type and Merge's minKeepSeq
// parameter), so older versions may legitimately need to stick around
// for a while. records passed to Write must therefore be sorted by
// (key ascending, then Seq descending within a key group) with no exact
// (key, Seq) duplicates - both invariants are maintained automatically
// by MemTable.All/Range and by Merge.
//
// Value compression: when enabled, a value is individually compressed
// with DEFLATE and the compressed-flag bit (0x80) is set on the record's
// type byte if doing so is actually smaller; otherwise the value is
// stored as-is. Compressing per-value (rather than per-block) keeps each
// record's byte offset in the index meaningful for direct seeks, at the
// cost of a worse compression ratio than block-level compression would
// give (no shared dictionary across records) - a deliberate simplicity
// trade-off for this project's scope.
package sstable

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"kvstore/memtable"

	"kvstore/bloom"
)

const magicNumber uint32 = 0x4B565353 // ASCII "KVSS"

// compressedFlag is OR-ed into a record's type byte when its value is
// stored DEFLATE-compressed. memtable.ValueType only currently uses the
// low bits (1 and 2), leaving the high bit free for this purpose.
const compressedFlag byte = 0x80

// dataHeaderSize is the fixed portion preceding each key/value pair in the
// data block: keyLen(4) + seq(8) + type(1) + valLen(4). valLen is always
// the length of the STORED bytes (i.e. the compressed length, if the
// compressed flag is set).
const dataHeaderSize = 4 + 8 + 1 + 4

// footerSize is the fixed trailer written at the very end of every table.
const footerSize = 8 + 8 + 8 + 8 + 8 + 4

// ErrEmptyMerge is returned by Merge when every input record was dropped
// (e.g. tombstones with no active snapshot needing them), leaving
// nothing to persist.
var ErrEmptyMerge = errors.New("sstable: merge produced no live records")

// Record is one logical key/value entry as stored in an SSTable. Value is
// always the LOGICAL (uncompressed) value; compression is an on-disk
// storage detail handled transparently by Write and Get/iteration.
type Record struct {
	Key   []byte
	Value []byte
	Type  memtable.ValueType
	Seq   uint64
}

type indexEntry struct {
	key    []byte
	offset uint64
}

// compressValue attempts to DEFLATE-compress v, returning the compressed
// bytes and true only if doing so actually produces something smaller;
// otherwise it returns v unchanged and false, so incompressible or tiny
// values are never inflated by compression overhead.
func compressValue(v []byte) ([]byte, bool) {
	if len(v) == 0 {
		return v, false
	}
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return v, false
	}
	if _, err := w.Write(v); err != nil {
		return v, false
	}
	if err := w.Close(); err != nil {
		return v, false
	}
	if buf.Len() >= len(v) {
		return v, false
	}
	return buf.Bytes(), true
}

func decompressValue(v []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(v))
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("decompressing value: %w", err)
	}
	return out, nil
}

// Write creates a brand-new, immutable SSTable at path containing exactly
// the given records, which MUST already be sorted by key ascending, then
// by Seq descending within a group of records sharing the same key (both
// guaranteed by MemTable.All/Range and by Merge), with no duplicate
// (key, Seq) pairs.
//
// If compress is true, each value is individually DEFLATE-compressed
// when doing so shrinks it (see compressValue).
//
// The table is written to a temporary file and atomically renamed into
// place only after every byte has been flushed and fsynced, so a reader
// (or a crash) can never observe a partially written table at its final
// path.
func Write(path string, records []Record, compress bool) error {
	if len(records) == 0 {
		return fmt.Errorf("sstable: refusing to write an empty table to %q", path)
	}

	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("sstable: create %q: %w", tmpPath, err)
	}

	abort := func(cause error) error {
		f.Close()
		os.Remove(tmpPath)
		return cause
	}

	w := bufio.NewWriterSize(f, 64*1024)
	var offset uint64

	write := func(b []byte) error {
		n, werr := w.Write(b)
		offset += uint64(n)
		if werr != nil {
			return fmt.Errorf("sstable: write to %q failed at offset %d: %w", tmpPath, offset, werr)
		}
		return nil
	}

	index := make([]indexEntry, 0, len(records))
	filter := bloom.NewForEntries(len(records), 0.01)

	var prevKey []byte
	var prevSeq uint64
	for i, r := range records {
		if prevKey != nil {
			c := bytes.Compare(prevKey, r.Key)
			if c > 0 || (c == 0 && r.Seq >= prevSeq) {
				return abort(fmt.Errorf("sstable: records passed to Write must be sorted by key ascending then Seq descending within a key group, with no duplicate (key,Seq) pairs; violated at index %d", i))
			}
		}
		prevKey, prevSeq = r.Key, r.Seq

		filter.Add(r.Key)
		index = append(index, indexEntry{key: r.Key, offset: offset})

		storedValue := r.Value
		typ := byte(r.Type)
		if compress && r.Type == memtable.TypeValue {
			if cv, ok := compressValue(r.Value); ok {
				storedValue = cv
				typ |= compressedFlag
			}
		}

		header := make([]byte, dataHeaderSize)
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(r.Key)))
		binary.LittleEndian.PutUint64(header[4:12], r.Seq)
		header[12] = typ
		binary.LittleEndian.PutUint32(header[13:17], uint32(len(storedValue)))

		if err := write(header); err != nil {
			return abort(err)
		}
		if err := write(r.Key); err != nil {
			return abort(err)
		}
		if len(storedValue) > 0 {
			if err := write(storedValue); err != nil {
				return abort(err)
			}
		}
	}

	indexOffset := offset
	for _, ie := range index {
		buf := make([]byte, 4+len(ie.key)+8)
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(ie.key)))
		copy(buf[4:4+len(ie.key)], ie.key)
		binary.LittleEndian.PutUint64(buf[4+len(ie.key):], ie.offset)
		if err := write(buf); err != nil {
			return abort(err)
		}
	}
	indexLen := offset - indexOffset

	bloomOffset := offset
	if err := write(filter.Encode()); err != nil {
		return abort(err)
	}
	bloomLen := offset - bloomOffset

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], indexLen)
	binary.LittleEndian.PutUint64(footer[16:24], bloomOffset)
	binary.LittleEndian.PutUint64(footer[24:32], bloomLen)
	binary.LittleEndian.PutUint64(footer[32:40], uint64(len(records)))
	binary.LittleEndian.PutUint32(footer[40:44], magicNumber)
	if err := write(footer); err != nil {
		return abort(err)
	}

	if err := w.Flush(); err != nil {
		return abort(fmt.Errorf("sstable: flush %q: %w", tmpPath, err))
	}
	if err := f.Sync(); err != nil {
		return abort(fmt.Errorf("sstable: fsync %q: %w", tmpPath, err))
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("sstable: close %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("sstable: rename %q -> %q: %w", tmpPath, path, err)
	}
	return nil
}
