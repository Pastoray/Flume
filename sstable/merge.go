package sstable

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"

	"kvstore/memtable"
)

// mergeItem is one live candidate record in the k-way merge, paired with
// the iterator it came from so it can be refilled after being popped.
type mergeItem struct {
	rec  Record
	iter *Iterator
}

// mergeHeap is a min-heap ordered by key, then by the record's own Seq
// descending so that when two items share a key, the one carrying the
// most recent write (by sequence number, assigned once at write time and
// never altered by compaction) is popped first.
type mergeHeap []*mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	c := bytes.Compare(h[i].rec.Key, h[j].rec.Key)
	if c != 0 {
		return c < 0
	}
	return h[i].rec.Seq > h[j].rec.Seq
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// Merge performs a k-way merge of EVERY version of every key across
// tables into a single new SSTable at outPath, compressing values if
// compress is true.
//
// minKeepSeq is the smallest Seq that any currently active snapshot
// might still need to read (pass ^uint64(0) if there are no active
// snapshots). For each key, versions are kept newest-to-oldest until one
// is found whose Seq already satisfies the oldest active snapshot (i.e.
// Seq <= minKeepSeq); everything strictly older than that point is
// guaranteed unreachable by any current reader and is dropped. This is
// the standard MVCC garbage-collection rule: only the run of versions
// down to the one covering the oldest live reader needs to survive.
//
// isBottommost must be true only if tables collectively represent every
// version of every key in their combined key range that exists anywhere
// in the store - i.e. there is no older, untouched table elsewhere that
// could still hold a shadowed value for one of these keys. Only then is
// it safe to drop a tombstone's own (newest-version) record once no
// active snapshot needs it: dropping it just means "no record found",
// which correctly represents "deleted" precisely because nothing older
// remains anywhere to incorrectly resurface.
//
// If every version of every key ends up dropped, Merge returns
// ErrEmptyMerge and does not create outPath.
func Merge(tables []*Table, outPath string, minKeepSeq uint64, isBottommost bool, compress bool) error {
	if len(tables) == 0 {
		return fmt.Errorf("sstable: cannot merge zero tables")
	}

	iters := make([]*Iterator, len(tables))
	for i, t := range tables {
		it, err := t.NewIterator()
		if err != nil {
			for _, opened := range iters[:i] {
				opened.Close()
			}
			return fmt.Errorf("sstable: merge: opening iterator for %q: %w", t.Path(), err)
		}
		iters[i] = it
	}
	defer func() {
		for _, it := range iters {
			if it != nil {
				it.Close()
			}
		}
	}()

	h := &mergeHeap{}
	heap.Init(h)
	for i, it := range iters {
		rec, ok, err := it.Next()
		if err != nil {
			return fmt.Errorf("sstable: merge: initial read from %q: %w", tables[i].Path(), err)
		}
		if ok {
			heap.Push(h, &mergeItem{rec: rec, iter: it})
		}
	}

	var merged []Record
	for h.Len() > 0 {
		top := heap.Pop(h).(*mergeItem)
		key := top.rec.Key

		// Gather every version of this key currently at the front of the
		// heap (across every source table), refilling from each source
		// as we consume it.
		group := []Record{top.rec}
		if nrec, ok, err := top.iter.Next(); err != nil {
			return fmt.Errorf("sstable: merge: read error: %w", err)
		} else if ok {
			heap.Push(h, &mergeItem{rec: nrec, iter: top.iter})
		}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.Key, key) {
			dup := heap.Pop(h).(*mergeItem)
			group = append(group, dup.rec)
			if nrec, ok, err := dup.iter.Next(); err != nil {
				return fmt.Errorf("sstable: merge: read error: %w", err)
			} else if ok {
				heap.Push(h, &mergeItem{rec: nrec, iter: dup.iter})
			}
		}

		// Sort defensively by Seq descending: this is the order the
		// heap's popping naturally produces across a SINGLE source, but
		// we don't rely on that alone across multiple sources - Write's
		// invariant demands a strict order regardless.
		sort.Slice(group, func(i, j int) bool { return group[i].Seq > group[j].Seq })

		// Walk this key's versions newest-to-oldest, keeping each one,
		// until we reach a version whose Seq already satisfies the
		// oldest active snapshot (Seq <= minKeepSeq) - at that point
		// every active snapshot (all of which have Seq >= minKeepSeq)
		// is guaranteed to already have found a satisfying version at
		// or above it, so anything strictly older is unreachable by any
		// current reader and can be dropped. This is the standard MVCC
		// garbage-collection rule: only the run of versions down to the
		// one covering the oldest live reader needs to survive.
		//
		// The newest version gets one extra option: if it's a
		// tombstone, this compaction is confirmed bottommost for this
		// key, AND it already satisfies minKeepSeq on its own (i.e. no
		// snapshot needs anything OLDER than it either), the tombstone
		// can be dropped outright rather than written - its absence
		// reads back as "not found" for any reader, which is exactly
		// right once nothing below it needs to survive regardless. If
		// some snapshot predates the tombstone (Seq > minKeepSeq), the
		// tombstone must still be physically kept: it's the only thing
		// that correctly "seals off" visibility into the older version
		// being retained below it for that snapshot - dropping it would
		// let an ordinary, unbounded read incorrectly see that older
		// value as if it were still live.
		for i, rec := range group {
			if i == 0 && isBottommost && rec.Type == memtable.TypeTombstone && rec.Seq <= minKeepSeq {
				break
			}
			merged = append(merged, rec)
			if rec.Seq <= minKeepSeq {
				break // oldest active snapshot (and thus all of them) already satisfied
			}
		}
	}

	if len(merged) == 0 {
		return ErrEmptyMerge
	}
	return Write(outPath, merged, compress)
}
