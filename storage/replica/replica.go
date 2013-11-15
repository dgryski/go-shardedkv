// Package replica provides a replicated storage backend
package replica

import (
	"github.com/dgryski/go-shardedkv"
	"math/rand"
	"sync"
)

type Storage struct {
	replicas []shardedkv.Storage
	mu       sync.Mutex
}

// New returns a Storage that queries multiple replicas in parallel
func New(replicas ...shardedkv.Storage) *Storage {
	return &Storage{
		replicas: replicas,
	}
}

func (s *Storage) Get(key string) ([]byte, bool, error) {

	l := len(s.replicas)

	if l == 1 {
		return s.replicas[0].Get(key)
	}

	idx1 := rand.Intn(l)
	idx2 := rand.Intn(l - 1)
	if idx2 >= idx1 {
		idx2++
	}
	r1 := s.replicas[idx1]
	r2 := s.replicas[idx2]

	type result struct {
		b   []byte
		ok  bool
		err error
	}

	f := func(storage shardedkv.Storage, ch chan<- result) {
		var r result
		r.b, r.ok, r.err = storage.Get(key)
		ch <- r
	}

	ch := make(chan result)
	go f(r1, ch)
	go f(r2, ch)

	r := <-ch

	if !r.ok || r.err != nil {
		// failed, try the other replica
		r := <-ch
		return r.b, r.ok, r.err
	}

	// success -- drain the other replica in the background
	go func() { <-ch }()

	return r.b, r.ok, r.err
}

func (s *Storage) Set(key string, val []byte) error {
	var err error
	for i := 0; i < len(s.replicas); i++ {
		e := s.replicas[i].Set(key, val)
		if err == nil && e != nil {
			err = e
		}
	}

	return err
}

func (s *Storage) Delete(key string) (bool, error) {
	var err error
	var ok bool
	for i := 0; i < len(s.replicas); i++ {
		o, e := s.replicas[i].Delete(key)

		ok = ok || o

		if err == nil && e != nil {
			err = e
		}
	}

	return ok, err
}

func (s *Storage) ResetConnection(key string) error {
	var err error
	for i := 0; i < len(s.replicas); i++ {
		e := s.replicas[i].ResetConnection(key)
		if err == nil && e != nil {
			err = e
		}
	}

	return err
}
