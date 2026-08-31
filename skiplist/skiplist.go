// Package skiplist implements a generic, ordered, concurrency-safe skip
// list keyed by byte slices. It backs the storage engine's MemTable.
//
// Concurrency model: a single sync.RWMutex guards the whole structure.
// Reads (Get, All, Len) take the read lock and can proceed fully in
// parallel with each other; writes (Put) take the exclusive write lock.
// This is the "localized RWLock" strategy for a fine-grained concurrent
// primitive - simpler and easier to reason about correctly than a
// lock-free skip list, while still allowing unlimited read concurrency,
// which is the dominant access pattern for a KV store's hot path.
package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel = 16
	pFactor  = 0.25
)

type node[V any] struct {
	key   []byte
	value V
	next  []*node[V]
}

// SkipList is an ordered map from byte-slice keys to values of type V.
// The zero value is not usable; construct with New.
type SkipList[V any] struct {
	mu     sync.RWMutex
	head   *node[V]
	level  int
	length int
	rnd    *rand.Rand
}

// New constructs an empty skip list.
func New[V any]() *SkipList[V] {
	return &SkipList[V]{
		head:  &node[V]{next: make([]*node[V], maxLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel picks a level for a newly inserted node using a geometric
// distribution, giving the classic skip-list expected O(log n) height.
func (s *SkipList[V]) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rnd.Float64() < pFactor {
		lvl++
	}
	return lvl
}

// Put inserts key/value, or overwrites the value if key already exists.
// The key is copied internally, so the caller's slice may be reused or
// mutated after Put returns.
func (s *SkipList[V]) Put(key []byte, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*node[V], maxLevel)
	cur := s.head
	for i := s.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		update[i] = cur
	}

	if next := cur.next[0]; next != nil && bytes.Equal(next.key, key) {
		next.value = value
		return
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := &node[V]{
		key:   append([]byte(nil), key...),
		value: value,
		next:  make([]*node[V], lvl),
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	s.length++
}

// Get returns the value associated with key, and whether it was found.
func (s *SkipList[V]) Get(key []byte) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cur := s.head
	for i := s.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
	}
	if next := cur.next[0]; next != nil && bytes.Equal(next.key, key) {
		return next.value, true
	}
	var zero V
	return zero, false
}

// Len returns the number of distinct keys currently stored.
func (s *SkipList[V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// KV pairs a key with a value, returned by All.
type KV[V any] struct {
	Key   []byte
	Value V
}

// All returns every entry in ascending key order. This is used when
// flushing a MemTable's contents into a sorted, immutable SSTable.
func (s *SkipList[V]) All() []KV[V] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]KV[V], 0, s.length)
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		out = append(out, KV[V]{Key: n.key, Value: n.value})
	}
	return out
}

// Range returns every entry with key >= start (or from the beginning, if
// start is nil) and key < end (or to the end, if end is nil), in
// ascending key order. Used to serve range scans directly from a
// MemTable without materializing its entire contents.
func (s *SkipList[V]) Range(start, end []byte) []KV[V] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cur := s.head
	for i := s.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && start != nil && bytes.Compare(cur.next[i].key, start) < 0 {
			cur = cur.next[i]
		}
	}

	var out []KV[V]
	for n := cur.next[0]; n != nil; n = n.next[0] {
		if end != nil && bytes.Compare(n.key, end) >= 0 {
			break
		}
		out = append(out, KV[V]{Key: n.key, Value: n.value})
	}
	return out
}
