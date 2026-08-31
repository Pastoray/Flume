package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"kvstore/memtable"
)

func tmpPath(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, name)
}

func TestWriteGetRoundTrip(t *testing.T) {
	path := tmpPath(t, "t1.sst")
	records := []Record{
		{Key: []byte("a"), Value: []byte("1"), Type: memtable.TypeValue, Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Type: memtable.TypeValue, Seq: 2},
		{Key: []byte("c"), Value: nil, Type: memtable.TypeTombstone, Seq: 3},
	}
	if err := Write(path, records, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	tbl, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer tbl.Release()

	rec, found, err := tbl.Get([]byte("a"), ^uint64(0))
	if err != nil || !found || string(rec.Value) != "1" {
		t.Fatalf("get a: rec=%+v found=%v err=%v", rec, found, err)
	}
	rec, found, err = tbl.Get([]byte("c"), ^uint64(0))
	if err != nil || !found || rec.Type != memtable.TypeTombstone {
		t.Fatalf("get c: rec=%+v found=%v err=%v", rec, found, err)
	}
	_, found, err = tbl.Get([]byte("missing"), ^uint64(0))
	if err != nil || found {
		t.Fatalf("get missing: found=%v err=%v", found, err)
	}
}

func TestMultiVersionGetRespectsMaxSeq(t *testing.T) {
	path := tmpPath(t, "t2.sst")
	// Three versions of the same key, newest first (as Write requires).
	records := []Record{
		{Key: []byte("k"), Value: []byte("v30"), Type: memtable.TypeValue, Seq: 30},
		{Key: []byte("k"), Value: []byte("v20"), Type: memtable.TypeValue, Seq: 20},
		{Key: []byte("k"), Value: []byte("v10"), Type: memtable.TypeValue, Seq: 10},
	}
	if err := Write(path, records, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	tbl, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer tbl.Release()

	cases := []struct {
		maxSeq uint64
		want   string
		found  bool
	}{
		{maxSeq: 100, want: "v30", found: true},
		{maxSeq: 30, want: "v30", found: true},
		{maxSeq: 25, want: "v20", found: true},
		{maxSeq: 20, want: "v20", found: true},
		{maxSeq: 15, want: "v10", found: true},
		{maxSeq: 10, want: "v10", found: true},
		{maxSeq: 5, want: "", found: false},
	}
	for _, c := range cases {
		rec, found, err := tbl.Get([]byte("k"), c.maxSeq)
		if err != nil {
			t.Fatalf("maxSeq=%d: %v", c.maxSeq, err)
		}
		if found != c.found {
			t.Fatalf("maxSeq=%d: found=%v want=%v", c.maxSeq, found, c.found)
		}
		if found && string(rec.Value) != c.want {
			t.Fatalf("maxSeq=%d: got %q want %q", c.maxSeq, rec.Value, c.want)
		}
	}
}

func TestWriteRejectsBadOrdering(t *testing.T) {
	path := tmpPath(t, "bad.sst")
	// Same key, seq ascending (wrong - must be descending within a group).
	records := []Record{
		{Key: []byte("k"), Value: []byte("v1"), Type: memtable.TypeValue, Seq: 1},
		{Key: []byte("k"), Value: []byte("v2"), Type: memtable.TypeValue, Seq: 2},
	}
	if err := Write(path, records, false); err == nil {
		t.Fatalf("expected error for bad ordering, got nil")
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("bad write should not have left a file behind")
	}
}

func TestRangeIterator(t *testing.T) {
	path := tmpPath(t, "range.sst")
	var records []Record
	for i := 0; i < 20; i++ {
		records = append(records, Record{
			Key:   []byte(fmt.Sprintf("k%02d", i)),
			Value: []byte(fmt.Sprintf("v%02d", i)),
			Type:  memtable.TypeValue,
			Seq:   uint64(i + 1),
		})
	}
	if err := Write(path, records, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	tbl, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer tbl.Release()

	ri := tbl.NewRangeIterator([]byte("k05"), []byte("k10"), ^uint64(0))
	var got []string
	for {
		rec, ok, err := ri.Next()
		if err != nil {
			t.Fatalf("range iterator: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(rec.Key))
	}
	want := []string{"k05", "k06", "k07", "k08", "k09"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestMergeKeepsTombstoneWhenOlderVersionRetainedForSnapshot(t *testing.T) {
	dir := t.TempDir()

	// History for key "k": v10 (put) -> v20 (put) -> v30 (tombstone).
	// An active snapshot pinned at seq=15 needs to see v10. Even though
	// this compaction is bottommost, the tombstone at v30 must NOT be
	// dropped: if it were, an ordinary (unbounded) read would
	// incorrectly find the retained-for-the-snapshot v10 as if it were
	// still live, instead of correctly seeing "deleted".
	pathA := filepath.Join(dir, "a.sst")
	if err := Write(pathA, []Record{
		{Key: []byte("k"), Value: []byte("v10"), Type: memtable.TypeValue, Seq: 10},
	}, false); err != nil {
		t.Fatalf("write a: %v", err)
	}
	pathB := filepath.Join(dir, "b.sst")
	if err := Write(pathB, []Record{
		{Key: []byte("k"), Value: []byte("v20"), Type: memtable.TypeValue, Seq: 20},
	}, false); err != nil {
		t.Fatalf("write b: %v", err)
	}
	pathC := filepath.Join(dir, "c.sst")
	if err := Write(pathC, []Record{
		{Key: []byte("k"), Type: memtable.TypeTombstone, Seq: 30},
	}, false); err != nil {
		t.Fatalf("write c: %v", err)
	}

	ta, err := Open(pathA)
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	defer ta.Release()
	tb, err := Open(pathB)
	if err != nil {
		t.Fatalf("open b: %v", err)
	}
	defer tb.Release()
	tc, err := Open(pathC)
	if err != nil {
		t.Fatalf("open c: %v", err)
	}
	defer tc.Release()

	outPath := filepath.Join(dir, "out.sst")
	if err := Merge([]*Table{ta, tb, tc}, outPath, 15, true, false); err != nil {
		t.Fatalf("merge: %v", err)
	}
	out, err := Open(outPath)
	if err != nil {
		t.Fatalf("open out: %v", err)
	}
	defer out.Release()

	// An unbounded (ordinary) read must see the key as deleted.
	rec, found, err := out.Get([]byte("k"), ^uint64(0))
	if err != nil {
		t.Fatalf("unbounded get: %v", err)
	}
	if !found || rec.Type != memtable.TypeTombstone {
		t.Fatalf("unbounded read should see the tombstone (deleted), got found=%v rec=%+v", found, rec)
	}

	// A read pinned at seq=15 (the snapshot's point in time) must still
	// see v10.
	rec, found, err = out.Get([]byte("k"), 15)
	if err != nil {
		t.Fatalf("snapshot@15 get: %v", err)
	}
	if !found || string(rec.Value) != "v10" {
		t.Fatalf("snapshot@15 should see v10, got found=%v rec=%+v", found, rec)
	}
}

func TestMergeDropsShadowedKeysAndTombstonesAtBottommost(t *testing.T) {
	dir := t.TempDir()

	pathA := filepath.Join(dir, "a.sst")
	if err := Write(pathA, []Record{
		{Key: []byte("k1"), Value: []byte("old"), Type: memtable.TypeValue, Seq: 1},
		{Key: []byte("k2"), Value: []byte("keep"), Type: memtable.TypeValue, Seq: 2},
	}, false); err != nil {
		t.Fatalf("write a: %v", err)
	}
	pathB := filepath.Join(dir, "b.sst")
	if err := Write(pathB, []Record{
		{Key: []byte("k1"), Type: memtable.TypeTombstone, Seq: 5}, // deletes k1
	}, false); err != nil {
		t.Fatalf("write b: %v", err)
	}

	ta, err := Open(pathA)
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	defer ta.Release()
	tb, err := Open(pathB)
	if err != nil {
		t.Fatalf("open b: %v", err)
	}
	defer tb.Release()

	outPath := filepath.Join(dir, "out.sst")
	// No active snapshots (minKeepSeq = max), bottommost = true: the
	// tombstone for k1 and its superseded old value should both vanish,
	// leaving only k2.
	if err := Merge([]*Table{ta, tb}, outPath, ^uint64(0), true, false); err != nil {
		t.Fatalf("merge: %v", err)
	}
	out, err := Open(outPath)
	if err != nil {
		t.Fatalf("open out: %v", err)
	}
	defer out.Release()

	if _, found, _ := out.Get([]byte("k1"), ^uint64(0)); found {
		t.Fatalf("k1 should have been fully dropped")
	}
	rec, found, err := out.Get([]byte("k2"), ^uint64(0))
	if err != nil || !found || string(rec.Value) != "keep" {
		t.Fatalf("k2: rec=%+v found=%v err=%v", rec, found, err)
	}
}

func TestMergeRetainsOlderVersionForActiveSnapshot(t *testing.T) {
	dir := t.TempDir()

	pathA := filepath.Join(dir, "a.sst")
	if err := Write(pathA, []Record{
		{Key: []byte("k"), Value: []byte("v10"), Type: memtable.TypeValue, Seq: 10},
	}, false); err != nil {
		t.Fatalf("write a: %v", err)
	}
	pathB := filepath.Join(dir, "b.sst")
	if err := Write(pathB, []Record{
		{Key: []byte("k"), Value: []byte("v20"), Type: memtable.TypeValue, Seq: 20},
	}, false); err != nil {
		t.Fatalf("write b: %v", err)
	}

	ta, err := Open(pathA)
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	defer ta.Release()
	tb, err := Open(pathB)
	if err != nil {
		t.Fatalf("open b: %v", err)
	}
	defer tb.Release()

	outPath := filepath.Join(dir, "out.sst")
	// An active snapshot pinned at seq=15 needs to still see v10 (the
	// version visible as of seq 15), even though v20 has since
	// superseded it. minKeepSeq=15 should force retention of v10.
	if err := Merge([]*Table{ta, tb}, outPath, 15, true, false); err != nil {
		t.Fatalf("merge: %v", err)
	}
	out, err := Open(outPath)
	if err != nil {
		t.Fatalf("open out: %v", err)
	}
	defer out.Release()

	rec, found, err := out.Get([]byte("k"), 15)
	if err != nil || !found || string(rec.Value) != "v10" {
		t.Fatalf("snapshot@15 should see v10: rec=%+v found=%v err=%v", rec, found, err)
	}
	rec, found, err = out.Get([]byte("k"), ^uint64(0))
	if err != nil || !found || string(rec.Value) != "v20" {
		t.Fatalf("unbounded read should see v20: rec=%+v found=%v err=%v", rec, found, err)
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	path := tmpPath(t, "compressed.sst")
	// A highly compressible value.
	bigVal := make([]byte, 4096)
	for i := range bigVal {
		bigVal[i] = 'x'
	}
	records := []Record{
		{Key: []byte("k"), Value: bigVal, Type: memtable.TypeValue, Seq: 1},
		{Key: []byte("small"), Value: []byte("a"), Type: memtable.TypeValue, Seq: 2},
	}
	if err := Write(path, records, true); err != nil {
		t.Fatalf("write: %v", err)
	}

	uncompressedPath := tmpPath(t, "uncompressed.sst")
	if err := Write(uncompressedPath, records, false); err != nil {
		t.Fatalf("write uncompressed: %v", err)
	}

	compStat, _ := os.Stat(path)
	plainStat, _ := os.Stat(uncompressedPath)
	if compStat.Size() >= plainStat.Size() {
		t.Fatalf("expected compressed file (%d bytes) to be smaller than uncompressed (%d bytes)", compStat.Size(), plainStat.Size())
	}

	tbl, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer tbl.Release()

	rec, found, err := tbl.Get([]byte("k"), ^uint64(0))
	if err != nil || !found || string(rec.Value) != string(bigVal) {
		t.Fatalf("compressed round-trip failed: found=%v err=%v len=%d", found, err, len(rec.Value))
	}
	rec, found, err = tbl.Get([]byte("small"), ^uint64(0))
	if err != nil || !found || string(rec.Value) != "a" {
		t.Fatalf("small value round-trip failed: rec=%+v found=%v err=%v", rec, found, err)
	}
}

func TestAcquireReleaseRetireLifecycle(t *testing.T) {
	path := tmpPath(t, "lifecycle.sst")
	if err := Write(path, []Record{{Key: []byte("a"), Value: []byte("1"), Type: memtable.TypeValue, Seq: 1}}, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	tbl, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if !tbl.Acquire() {
		t.Fatalf("acquire should succeed")
	}
	// refcount now 2 (initial + acquired). Retire (drops the initial ref
	// and marks for deletion) should NOT close/delete yet since we still
	// hold our acquired reference.
	if err := tbl.Retire(); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file should still exist while a reference is outstanding: %v", err)
	}
	// Still usable while we hold our reference.
	if _, _, err := tbl.Get([]byte("a"), ^uint64(0)); err != nil {
		t.Fatalf("get while referenced after retire: %v", err)
	}
	if err := tbl.Release(); err != nil {
		t.Fatalf("final release: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file should be deleted after last reference released, stat err=%v", err)
	}
	if tbl.Acquire() {
		t.Fatalf("acquire should fail after full release")
	}
}
