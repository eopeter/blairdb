package internals

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"hash/fnv"
)

type bloomFilter struct {
	bits    []uint64
	hashers []func() hash.Hash64
}

func (b *bloomFilter) Add(key []byte) {
	sem := make(chan int, len(b.hashers))
	for _, f := range b.hashers {
		sem <- 1
		go func(h hash.Hash64) {
			_, _ = h.Write(key)
			p := h.Sum64() % uint64(len(b.bits))
			b.bits[p] = 1
			<-sem
		}(f())
	}
}

func (b *bloomFilter) HasKey(key []byte) bool {
	positiveBits := 0
	for _, f := range b.hashers {
		h := f()
		_, _ = h.Write(key)
		p := h.Sum64() % uint64(len(b.bits))
		if b.bits[p] == 1 {
			positiveBits++
		} else {
			return false
		}
	}
	return positiveBits == len(b.hashers)
}

func NewBloomFilter(bitCount int) BloomFilter {
	return &bloomFilter{
		bits: make([]uint64, bitCount), //m = 18
		hashers: []func() hash.Hash64{ //k = 2
			func() hash.Hash64 {
				return fnv.New64a()
			},
			func() hash.Hash64 {
				return murmur3.New64()
			},
		},
	}
}

func NewBloomFilterFromKeys(keys [][]byte, bitCount int) BloomFilter {
	filter := NewBloomFilter(bitCount)
	for _, key := range keys {
		filter.Add(key)
	}
	return filter
}
