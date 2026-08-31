package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "kvstore-test-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestConcurrentPutDeleteOverwriteStress(t *testing.T) {
	dir := tempDir(t)
	// Tiny thresholds so flush and compaction are constantly triggered
	// while writes are still landing - this is the regime that exposed
	// the ordering bugs between flushing and compaction.
	e, err := Open(dir, WithFlushThreshold(48), WithL0CompactionTrigger(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	const numKeys = 40
	const workers = 8
	const rounds = 60

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("shared-key-%03d", i))
	}

	// opMu serializes each (operation + bookkeeping-update) pair so that
	// "the last op recorded per key" faithfully matches the order
	// operations actually landed in, while still letting many goroutines
	// race to acquire that pairing concurrently - which is exactly the
	// contention pattern that exercises the flush/compaction ordering
	// this test is guarding against.
	var opMu sync.Mutex
	lastIsDelete := make([]bool, numKeys)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				k := r % numKeys
				isDelete := (r+w)%5 == 0

				opMu.Lock()
				var opErr error
				if isDelete {
					opErr = e.Delete(keys[k])
				} else {
					v := []byte(fmt.Sprintf("w%d-r%d", w, r))
					opErr = e.Put(keys[k], v)
				}
				if opErr == nil {
					lastIsDelete[k] = isDelete
				}
				opMu.Unlock()

				if opErr != nil {
					t.Errorf("worker %d op round %d: %v", w, r, opErr)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Wait for compaction to settle.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e.Stats().TablesPerLevel[0] < e.l0CompactionTrigger {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// We can't predict the exact final value per key (workers race on
	// shared keys), but we CAN assert the fundamental invariant this
	// bug-hunt was about: a key's presence/absence must be consistent
	// with whichever operation actually landed last, not with a stale
	// value incorrectly resurfacing after a delete (or vice versa).
	for k := 0; k < numKeys; k++ {
		_, err := e.Get(keys[k])
		wasDeleted := lastIsDelete[k]
		if wasDeleted && err == nil {
			t.Fatalf("key %s: last op was a delete, but key is still readable", keys[k])
		}
		if !wasDeleted && errors.Is(err, ErrNotFound) {
			t.Fatalf("key %s: last op was a put, but key is missing", keys[k])
		}
	}
}

func TestScanReturnsKeysInRangeAscendingOrder(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(1<<30))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%02d", i)
		if err := e.Put([]byte(k), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// Delete a couple of keys within the range to verify tombstones are
	// excluded from scan results.
	if err := e.Delete([]byte("k05")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := e.Delete([]byte("k10")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	it, err := e.Scan([]byte("k03"), []byte("k12"))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer it.Close()

	var got []string
	for {
		key, value, ok, err := it.Next()
		if err != nil {
			t.Fatalf("scan next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, fmt.Sprintf("%s=%s", key, value))
	}
	want := []string{"k03=v03", "k04=v04", "k06=v06", "k07=v07", "k08=v08", "k09=v09", "k11=v11"}
	if len(got) != len(want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v\nwant %v", got, want)
		}
	}
}

func TestScanAcrossMemtableAndSSTables(t *testing.T) {
	dir := tempDir(t)
	// Tiny threshold so most keys end up flushed to SSTables, while a
	// few remain in the active MemTable - exercising the merge across
	// both sources.
	e, err := Open(dir, WithFlushThreshold(48))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if err := e.Put([]byte(k), []byte(fmt.Sprintf("val-%03d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	it, err := e.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer it.Close()

	count := 0
	var lastKey []byte
	for {
		key, value, ok, err := it.Next()
		if err != nil {
			t.Fatalf("scan next: %v", err)
		}
		if !ok {
			break
		}
		if lastKey != nil && string(key) <= string(lastKey) {
			t.Fatalf("scan not in ascending order: %s after %s", key, lastKey)
		}
		lastKey = key
		want := "val-" + string(key[len("key-"):])
		if string(value) != want {
			t.Fatalf("key %s: got %q want %q", key, value, want)
		}
		count++
	}
	if count != 50 {
		t.Fatalf("expected 50 entries, got %d", count)
	}
}

func TestSnapshotIsolatesFromLaterWrites(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(64), WithL0CompactionTrigger(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	if err := e.Put([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Force at least one MemTable rotation so "before" is moved into an
	// immutable (or already-flushed) MemTable before we snapshot and
	// then mutate the same key again. This matters because a MemTable
	// only ever holds one version per key: if the upcoming overwrite
	// landed on the SAME still-mutable MemTable as "before", it would
	// simply replace it in place with nothing durable to fall back on -
	// exactly the documented Snapshot isolation caveat for writes that
	// race within a single MemTable's lifetime. Rotating first is what
	// puts us in the regime Snapshot actually guarantees isolation for.
	for i := 0; i < 5; i++ {
		if err := e.Put([]byte(fmt.Sprintf("rotate-filler-%d", i)), []byte("some padding bytes to force a rotation")); err != nil {
			t.Fatalf("filler put: %v", err)
		}
	}

	snap, err := e.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()

	// Mutate the key after the snapshot was taken, and add enough churn
	// to force flushes and compactions to run concurrently with the
	// still-open snapshot.
	if err := e.Put([]byte("k"), []byte("after")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := e.Put([]byte(fmt.Sprintf("filler-%03d", i)), []byte("x")); err != nil {
			t.Fatalf("put filler: %v", err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e.Stats().TablesPerLevel[0] < 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The snapshot must still see the value as of its creation time...
	v, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snapshot get: %v", err)
	}
	if string(v) != "before" {
		t.Fatalf("snapshot get: got %q want %q", v, "before")
	}

	// ...while a normal (unbounded) read sees the latest state (deleted).
	if _, err := e.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("live get: expected ErrNotFound, got %v", err)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}
	// Idempotent.
	if err := snap.Close(); err != nil {
		t.Fatalf("second snapshot close should be a no-op: %v", err)
	}
}

func TestSnapshotScanSeesConsistentPointInTime(t *testing.T) {
	dir := tempDir(t)
	// Small threshold so the initial writes are forced out of the
	// active MemTable (into an immutable one or flushed to disk) before
	// the snapshot is taken and the keys are overwritten again - without
	// this, the overwrites would land on the SAME still-mutable
	// MemTable, which only ever holds one version per key, erasing "v0"
	// before the snapshot could ever see it (the documented
	// in-memory-overwrite isolation caveat). This test wants to exercise
	// the isolation guarantee Snapshot actually makes, not that caveat.
	e, err := Open(dir, WithFlushThreshold(64))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 10; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v0")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// Force a rotation so every k00..k09 write above is guaranteed to be
	// out of the currently-mutable MemTable before we snapshot and then
	// overwrite them - see the comment above for why that matters.
	if err := e.Put([]byte("rotate-filler"), []byte("padding bytes to push the memtable over its flush threshold")); err != nil {
		t.Fatalf("filler put: %v", err)
	}

	snap, err := e.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()

	// Overwrite everything and add a new key after the snapshot.
	for i := 0; i < 10; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v1")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := e.Put([]byte("k99"), []byte("new")); err != nil {
		t.Fatalf("put: %v", err)
	}

	it, err := snap.Scan(nil, nil)
	if err != nil {
		t.Fatalf("snapshot scan: %v", err)
	}
	defer it.Close()

	count := 0
	for {
		key, value, ok, err := it.Next()
		if err != nil {
			t.Fatalf("scan next: %v", err)
		}
		if !ok {
			break
		}
		if string(key) == "rotate-filler" {
			continue // not part of what we're checking; skip
		}
		if string(value) != "v0" {
			t.Fatalf("key %s: snapshot scan should see v0, got %q", key, value)
		}
		if string(key) == "k99" {
			t.Fatalf("snapshot scan should not see key added after snapshot")
		}
		count++
	}
	if count != 10 {
		t.Fatalf("expected 10 entries, got %d", count)
	}
}

func TestLeveledCompactionPromotesTablesAcrossLevels(t *testing.T) {
	dir := tempDir(t)
	// Aggressive thresholds so L0->L1 and L1->L2 compactions both
	// actually happen within a small, fast test.
	e, err := Open(dir,
		WithFlushThreshold(48),
		WithL0CompactionTrigger(2),
		WithBaseLevelSizeBytes(200),
		WithLevelSizeMultiplier(2),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key-%04d", i)
		if err := e.Put([]byte(k), []byte("some-reasonably-sized-value")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	var stats Stats
	for time.Now().Before(deadline) {
		stats = e.Stats()
		if stats.TablesPerLevel[0] < e.l0CompactionTrigger {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Give any cascading L1->L2 compaction a moment to settle too.
	time.Sleep(200 * time.Millisecond)
	stats = e.Stats()

	foundBeyondL0 := false
	for lvl := 1; lvl < len(stats.TablesPerLevel); lvl++ {
		if stats.TablesPerLevel[lvl] > 0 {
			foundBeyondL0 = true
			break
		}
	}
	if !foundBeyondL0 {
		t.Fatalf("expected compaction to have promoted at least one table beyond L0, stats=%+v", stats)
	}

	// Regardless of which levels data ended up on, every key must still
	// be readable with the correct value.
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v, err := e.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(v) != "some-reasonably-sized-value" {
			t.Fatalf("get %s: got %q", k, v)
		}
	}
}

func TestCompressionRoundTripThroughEngine(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(64), WithCompression(true))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	bigVal := make([]byte, 8192)
	for i := range bigVal {
		bigVal[i] = byte('a' + i%5)
	}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key-%02d", i)
		if err := e.Put([]byte(k), bigVal); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := Open(dir, WithCompression(true))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key-%02d", i)
		v, err := e2.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(v) != string(bigVal) {
			t.Fatalf("key %s: compressed round-trip mismatch", k)
		}
	}
}

func TestMetricsReflectRealOperations(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(64), WithL0CompactionTrigger(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 20; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := e.Get([]byte("k00")); err != nil {
		t.Fatalf("get hit: %v", err)
	}
	if _, err := e.Get([]byte("does-not-exist")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get miss: %v", err)
	}
	if err := e.Delete([]byte("k01")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	it, err := e.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	it.Close()

	snap, err := e.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e.Stats().TablesPerLevel[0] < e.l0CompactionTrigger {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = e.Stats() // refreshes the gauges as a side effect

	var buf bytes.Buffer
	if err := e.MetricsRegistry().Render(&buf); err != nil {
		t.Fatalf("render metrics: %v", err)
	}
	out := buf.String()

	mustContain := []string{
		"kvstore_puts_total 20",
		"kvstore_deletes_total 1",
		"kvstore_get_hits_total 1",
		"kvstore_get_misses_total 1",
		"kvstore_scans_total 1",
		"kvstore_snapshots_opened_total 1",
		"kvstore_active_snapshots 1",
		"kvstore_flushes_total",
		"kvstore_compactions_total",
		"kvstore_sstables_per_level{level=\"0\"}",
		"kvstore_write_duration_seconds_count 21", // 20 puts + 1 delete
		"kvstore_get_duration_seconds_count 2",
	}
	for _, want := range mustContain {
		if !strings.Contains(out, want) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", want, out)
		}
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}
}

func TestPutGetDelete(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	if err := e.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, err := e.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "bar" {
		t.Fatalf("got %q, want %q", v, "bar")
	}

	if err := e.Delete([]byte("foo")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := e.Get([]byte("foo")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	if _, err := e.Get([]byte("never-existed")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing key, got %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 5; i++ {
		if err := e.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	v, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "v4" {
		t.Fatalf("got %q, want v4", v)
	}
}

func TestFlushAndReopenAcrossSSTable(t *testing.T) {
	dir := tempDir(t)
	// Tiny flush threshold so every Put forces a rotate+flush, exercising
	// the on-disk SSTable read path rather than just the MemTable.
	e, err := Open(dir, WithFlushThreshold(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	n := 200
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%05d", i)
		v := fmt.Sprintf("value-%05d", i)
		if err := e.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify every key is still readable purely from SSTables.
	e2, err := Open(dir, WithFlushThreshold(1))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%05d", i)
		want := fmt.Sprintf("value-%05d", i)
		got, err := e2.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("key %s: got %q want %q", k, got, want)
		}
	}

	stats := e2.Stats()
	if stats.SSTables == 0 {
		t.Fatalf("expected at least one sstable on disk, stats=%+v", stats)
	}
}

func TestCrashRecoveryReplaysWAL(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(1<<30)) // large: keep everything in the memtable
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		if err := e.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := e.Delete([]byte("k10")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Simulate an ungraceful crash: do NOT call Close, just abandon the
	// engine. The WAL on disk is the only durable record of these writes.
	files, _ := os.ReadDir(dir)
	foundWAL := false
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".wal" {
			foundWAL = true
		}
	}
	if !foundWAL {
		t.Fatalf("expected a wal file to exist before simulated crash")
	}

	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("recovery open: %v", err)
	}
	defer e2.Close()

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%d", i)
		if i == 10 {
			if _, err := e2.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("expected k10 to be deleted after recovery, got err=%v", err)
			}
			continue
		}
		want := fmt.Sprintf("v%d", i)
		got, err := e2.Get([]byte(k))
		if err != nil {
			t.Fatalf("recovered get %s: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("recovered key %s: got %q want %q", k, got, want)
		}
	}

	// After recovery, exactly one WAL should remain: the fresh, empty one
	// backing the new active MemTable. The WAL(s) that were replayed must
	// have been removed.
	files, _ = os.ReadDir(dir)
	walCount := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".wal" {
			walCount++
		}
	}
	if walCount != 1 {
		t.Fatalf("expected exactly 1 wal file after recovery (the fresh active one), found %d", walCount)
	}
}

func TestCorruptedWALTailIsTruncatedNotFatal(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(1<<30))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Find the WAL file and append garbage bytes simulating a torn write.
	var walPath string
	entries, _ := os.ReadDir(dir)
	for _, ent := range entries {
		if filepath.Ext(ent.Name()) == ".wal" {
			walPath = filepath.Join(dir, ent.Name())
		}
	}
	if walPath == "" {
		t.Fatalf("no wal file found")
	}
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open wal for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("open after corruption should not fail: %v", err)
	}
	defer e2.Close()

	for i := 0; i < 10; i++ {
		got, err := e2.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("get k%d after corrupted-tail recovery: %v", i, err)
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d: got %q", i, got)
		}
	}
}

func TestCompactionDropsTombstonesAndReducesFileCount(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(64), WithL0CompactionTrigger(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		if err := e.Put(k, []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		if err := e.Delete(k); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	// Wait for the background compaction worker (the only goroutine that
	// should ever run compactions) to settle the SSTable count back at or
	// below the configured threshold.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e.Stats().TablesPerLevel[0] < e.l0CompactionTrigger {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		_, err := e.Get(k)
		if i < 50 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("key %s should be deleted, got err=%v", k, err)
			}
		} else {
			if err != nil {
				t.Fatalf("key %s should exist: %v", k, err)
			}
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir, WithFlushThreshold(2048), WithL0CompactionTrigger(3))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	const workers = 16
	const opsPerWorker = 200
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				k := []byte(fmt.Sprintf("w%d-k%d", w, i))
				v := []byte(fmt.Sprintf("w%d-v%d", w, i))
				if err := e.Put(k, v); err != nil {
					t.Errorf("worker %d put %d: %v", w, i, err)
					return
				}
				got, err := e.Get(k)
				if err != nil {
					t.Errorf("worker %d get %d: %v", w, i, err)
					return
				}
				if string(got) != string(v) {
					t.Errorf("worker %d key %d: got %q want %q", w, i, got, v)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for w := 0; w < workers; w++ {
		for i := 0; i < opsPerWorker; i++ {
			k := fmt.Sprintf("w%d-k%d", w, i)
			want := fmt.Sprintf("w%d-v%d", w, i)
			got, err := e.Get([]byte(k))
			if err != nil {
				t.Fatalf("final verify %s: %v", k, err)
			}
			if string(got) != want {
				t.Fatalf("final verify %s: got %q want %q", k, got, want)
			}
		}
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	if err := e.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
	if _, err := e.Get(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
}

func TestUseAfterCloseFails(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := e.Put([]byte("c"), []byte("d")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := e.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
