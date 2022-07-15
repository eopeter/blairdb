package internals

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"hash/fnv"
)

type bloomFilter struct {
	bits    []uint64
	hashers []hash.Hash64
}

func (b bloomFilter) Add(key []byte) {
	for _, f := range b.hashers {
		_, _ = f.Write(key)
		p := f.Sum64() % uint64(len(b.bits))
		b.bits[p] = 1
	}
}

func (b bloomFilter) HasKey(key []byte) bool {
	positiveBits := 0
	for _, f := range b.hashers {
		_, _ = f.Write(key)
		p := f.Sum64() % uint64(len(b.bits))
		if b.bits[p] == 1 {
			positiveBits++
		} else {
			return false
		}
	}
	return positiveBits == len(b.hashers)
}

func NewBloomFilter() BloomFilter {
	return &bloomFilter{
		bits: make([]uint64, 18), //m = 18
		hashers: []hash.Hash64{ //k = 2
			fnv.New64a(),
			murmur3.New64(),
		},
	}
}
