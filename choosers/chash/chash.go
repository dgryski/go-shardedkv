package chash

import (
	"github.com/golang/groupcache/consistenthash"
)

type CHash struct {
	m *consistenthash.Map
}

func New(servers ...string) *CHash {

	c := &CHash{
		m: consistenthash.New(160, leveldbHash),
	}

	c.m.Add(servers...)

	return c
}

func (c *CHash) Choose(key string) string { return c.m.Get(key) }

// leveldb's bloom filter hash, a murmur-lite
func leveldbHash(b []byte) uint32 {

	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)

	h := uint32(seed) ^ uint32(len(b)*m)

	for ; len(b) >= 4; b = b[4:] {

		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}

	return h
}
