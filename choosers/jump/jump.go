// Package jump is a chooser using Google's Jump Consistent Hash.  It requires a 64-bit hash function for the string-to-uint64 mapping.  Good candidates are siphash, spooky, cityhash and farmhash.
package jump

import "github.com/dgryski/go-jump"

type Jump struct {
	hasher func([]byte) uint64
	nodes  []string
}

func New(h func([]byte) uint64) *Jump {
	return &Jump{hasher: h}
}

func (j *Jump) SetBuckets(buckets []string) error {
	j.nodes = append(j.nodes, buckets...)
	return nil
}

func (j *Jump) Choose(key string) string {
	// Hard-coded spooky hash for now.  Easy enough to replace if needed.
	return j.nodes[jump.Hash(j.hasher([]byte(key)), len(j.nodes))]
}

func (j *Jump) Buckets() []string { return j.nodes }
