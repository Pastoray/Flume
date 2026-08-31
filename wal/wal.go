// Package wal implements a strict, append-only write-ahead log used to
// guarantee durability of writes before they are applied to the in-memory
// MemTable. Every record is checksummed with CRC32 so that corruption or a
// torn write caused by a crash mid-append can be detected during recovery.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// RecordType distinguishes a value write from a tombstone (deletion).
type RecordType byte

const (
	RecordPut    RecordType = 1
	RecordDelete RecordType = 2
)

// Record is a single logical mutation appended to the log.
type Record struct {
	Type  RecordType
	Key   []byte
	Value []byte
	Seq   uint64 // monotonically increasing global sequence number
}

// On-disk record layout (all integers little-endian):
//
//	[4 bytes]  CRC32 checksum, computed over everything below
//	[1 byte]   record type
//	[8 bytes]  sequence number
//	[4 bytes]  key length
//	[4 bytes]  value length
//	[N bytes]  key
//	[M bytes]  value
const headerSize = 4 + 1 + 8 + 4 + 4

// WAL is an append-only log file. All exported methods are safe for
// concurrent use.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
	path string
}

// Open opens (creating if necessary) the WAL file at path in append mode.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open %q: %w", path, err)
	}
	return &WAL{
		file: f,
		w:    bufio.NewWriterSize(f, 64*1024),
		path: path,
	}, nil
}

// Append serializes rec, writes it, flushes the userspace buffer, and
// fsyncs the underlying file descriptor before returning. A nil error
// return is a durability guarantee: the record will survive a subsequent
// crash or power loss.
func (w *WAL) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	total := headerSize + len(rec.Key) + len(rec.Value)
	buf := make([]byte, total)

	buf[4] = byte(rec.Type)
	binary.LittleEndian.PutUint64(buf[5:13], rec.Seq)
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(rec.Key)))
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(rec.Value)))
	copy(buf[headerSize:], rec.Key)
	copy(buf[headerSize+len(rec.Key):], rec.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	if _, err := w.w.Write(buf); err != nil {
		return fmt.Errorf("wal: write to %q failed: %w", w.path, err)
	}
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush of %q failed: %w", w.path, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync of %q failed: %w", w.path, err)
	}
	return nil
}

// Close flushes any buffered data and closes the underlying file
// descriptor. It does not delete the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: final flush of %q failed: %w", w.path, err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: close of %q failed: %w", w.path, err)
	}
	return nil
}

// Path returns the filesystem path backing this log.
func (w *WAL) Path() string { return w.path }

// Remove deletes the WAL file at path. It is used once every record it
// contains has been durably persisted into an SSTable, so the log itself
// is no longer needed for crash recovery.
func Remove(path string) error {
	return os.Remove(path)
}

// Replay reads every well-formed record sequentially from the WAL file at
// path and invokes fn for each one, in the order they were written.
//
// If the file does not exist, Replay is a silent no-op (there is nothing to
// recover). If a corrupted or partially-written record is encountered -
// which can legitimately happen if the process crashed mid-append - replay
// stops at that point without error: every record successfully parsed
// before the corruption point is still valid and has already been
// delivered to fn. This mirrors standard LSM-tree WAL recovery semantics,
// where a torn tail write is discarded rather than treated as fatal.
func Replay(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("wal: failed to open %q for replay: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	for {
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Clean end-of-file, or a torn header at the tail caused by
				// a crash mid-write. Either way, recovery is done.
				return nil
			}
			return fmt.Errorf("wal: reading header from %q: %w", path, err)
		}

		crc := binary.LittleEndian.Uint32(header[0:4])
		typ := RecordType(header[4])
		seq := binary.LittleEndian.Uint64(header[5:13])
		keyLen := binary.LittleEndian.Uint32(header[13:17])
		valLen := binary.LittleEndian.Uint32(header[17:21])

		// Guard against a corrupted header reporting an absurd length,
		// which would otherwise trigger a huge, wasted allocation.
		const maxReasonableLen = 512 * 1024 * 1024
		if uint64(keyLen) > maxReasonableLen || uint64(valLen) > maxReasonableLen {
			return nil
		}

		body := make([]byte, uint64(keyLen)+uint64(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			// A torn body write at the tail of the file: the record header
			// was flushed but the payload was not (or only partially was)
			// before the crash. Discard it and stop.
			return nil
		}

		check := make([]byte, 1+8+4+4+len(body))
		check[0] = byte(typ)
		binary.LittleEndian.PutUint64(check[1:9], seq)
		binary.LittleEndian.PutUint32(check[9:13], keyLen)
		binary.LittleEndian.PutUint32(check[13:17], valLen)
		copy(check[17:], body)

		if crc32.ChecksumIEEE(check) != crc {
			// Checksum mismatch: the record is corrupt. Rather than risk
			// applying bad data (or misinterpreting subsequent bytes as a
			// new record), stop replay here.
			return nil
		}

		rec := Record{
			Type:  typ,
			Seq:   seq,
			Key:   body[:keyLen],
			Value: body[keyLen:],
		}
		if err := fn(rec); err != nil {
			return fmt.Errorf("wal: apply callback for record in %q: %w", path, err)
		}
	}
}
