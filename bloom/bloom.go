// Package bloom implements a standard Bloom filter. Each SSTable maintains
// one so reads can cheaply determine that a key is definitely absent from
// that table without touching disk, which is critical for read
// performance once many SSTables have accumulated.
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// Filter is a fixed-size bit array tested with k independent hash
// functions, derived here via double hashing (Kirsch-Mitzenmacher) from
// two underlying FNV hashes so we avoid computing k full hash passes.
type Filter struct {
	bits    []byte
	numBits uint32
	numHash uint32
}

// NewForEntries sizes a filter for n expected entries at the target false
// positive rate fp, using the standard optimal-parameter formulas:
//
//	m = ceil(-n * ln(fp) / ln(2)^2)   (bits)
//	k = round((m / n) * ln(2))        (hash functions)
func NewForEntries(n int, fp float64) *Filter {
	if n < 1 {
		n = 1
	}
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	m := math.Ceil(-1 * float64(n) * math.Log(fp) / (math.Ln2 * math.Ln2))
	k := math.Round((m / float64(n)) * math.Ln2)
	if k < 1 {
		k = 1
	}
	numBits := uint32(m)
	if numBits == 0 {
		numBits = 1
	}
	return &Filter{
		bits:    make([]byte, (numBits+7)/8),
		numBits: numBits,
		numHash: uint32(k),
	}
}

// hashes returns two independent 32-bit hashes of key, used as the basis
// for the k derived hash functions.
func hashes(key []byte) (uint32, uint32) {
	h1 := fnv.New32a()
	h1.Write(key)
	sum1 := h1.Sum32()

	h2 := fnv.New32()
	h2.Write(key)
	sum2 := h2.Sum32()

	return sum1, sum2
}

// Add records key as present in the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := hashes(key)
	for i := uint32(0); i < f.numHash; i++ {
		idx := (h1 + i*h2) % f.numBits
		f.bits[idx/8] |= 1 << (idx % 8)
	}
}

// MayContain reports whether key might be present. false is a definite
// answer (the key is absent); true means the key might be present and the
// caller must check the underlying data to be sure.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := hashes(key)
	for i := uint32(0); i < f.numHash; i++ {
		idx := (h1 + i*h2) % f.numBits
		if f.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the filter for embedding in an SSTable's footer block.
func (f *Filter) Encode() []byte {
	buf := make([]byte, 4+4+len(f.bits))
	binary.LittleEndian.PutUint32(buf[0:4], f.numBits)
	binary.LittleEndian.PutUint32(buf[4:8], f.numHash)
	copy(buf[8:], f.bits)
	return buf
}

// Decode reconstructs a Filter previously produced by Encode.
func Decode(buf []byte) (*Filter, error) {
	if len(buf) < 8 {
		return nil, fmt.Errorf("bloom: buffer too short (%d bytes) to contain a valid filter header", len(buf))
	}
	numBits := binary.LittleEndian.Uint32(buf[0:4])
	numHash := binary.LittleEndian.Uint32(buf[4:8])
	expectedBytes := int((numBits + 7) / 8)
	if len(buf)-8 != expectedBytes {
		return nil, fmt.Errorf("bloom: corrupt filter: expected %d bit-array bytes, found %d", expectedBytes, len(buf)-8)
	}
	bits := append([]byte(nil), buf[8:]...)
	return &Filter{bits: bits, numBits: numBits, numHash: numHash}, nil
}
