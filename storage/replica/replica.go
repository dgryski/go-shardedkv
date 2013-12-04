// Package replica provides a replicated storage backend
package replica

import (
	"fmt"
	"math/rand"
	"strings"

	shardedkv "github.com/dgryski/go-shardedkv"
)

type Storage struct {
	MaxFailures int
	Replicas    []shardedkv.Storage
}

type ReplicaError struct {
	Replica int
	Err     error
}

func (r ReplicaError) Error() string {
	return fmt.Sprintf("replica %d: %s", r.Replica, r.Err)
}

type MultiError []ReplicaError

func (me MultiError) Error() string {

	var errs []string
	for _, e := range me {
		errs = append(errs, e.Error())
	}

	return strings.Join(errs, ";")
}

// New returns a Storage that queries multiple replicas in parallel
func New(maxFailures int, replicas ...shardedkv.Storage) *Storage {
	return &Storage{
		MaxFailures: maxFailures,
		Replicas:    replicas,
	}
}

func (s *Storage) Get(key string) ([]byte, bool, error) {

	l := len(s.Replicas)

	if l == 1 {
		return s.Replicas[0].Get(key)
	}

	idx1 := rand.Intn(l)
	idx2 := rand.Intn(l - 1)
	if idx2 >= idx1 {
		idx2++
	}
	r1 := s.Replicas[idx1]
	r2 := s.Replicas[idx2]

	type result struct {
		idx int
		b   []byte
		ok  bool
		err error
	}

	f := func(idx int, storage shardedkv.Storage, ch chan<- result) {
		var r result
		r.idx = idx
		r.b, r.ok, r.err = storage.Get(key)
		ch <- r
	}

	ch := make(chan result)
	go f(idx1, r1, ch)

	// TODO(dgryski): hold off sending the second request until the first
	// one has been outstanding for more than the 95th-percentile expected
	// latency.
	// http://cacm.acm.org/magazines/2013/2/160173-the-tail-at-scale/fulltext
	go f(idx2, r2, ch)

	r := <-ch

	if r.ok && r.err == nil {
		// success!
		// drain the other replica in the background
		go func() { <-ch }()

		// and return what we got
		return r.b, true, nil
	}

	// store this error away
	// note that r.err might actually be nil here
	rerr := ReplicaError{Replica: r.idx, Err: r.err}

	// try the other replica
	r = <-ch

	if r.ok && r.err == nil {
		// success!

		// Return the first error if we had one _and_ if the user wants
		// to be notified of it.  It's wrapped in a MultiError so we're
		// consistent about what we return when dealing with the
		// replicas.
		if rerr.Err != nil && s.MaxFailures == 0 {
			return r.b, r.ok, MultiError{rerr}
		}

		return r.b, r.ok, nil
	}

	var me MultiError

	// add the error from our first request, if it wasn't just a failed get
	if rerr.Err != nil {
		me = append(me, rerr)
	}

	// add the error from the second request, if any
	if r.err != nil {
		me = append(me, ReplicaError{Replica: r.idx, Err: r.err})
	}

	// finally, only report the errors if we've above the MaxFailures limit
	if len(me) > s.MaxFailures {
		return r.b, r.ok, me
	}

	return r.b, r.ok, nil
}

func (s *Storage) Set(key string, val []byte) error {
	var merr MultiError
	for i := 0; i < len(s.Replicas); i++ {
		err := s.Replicas[i].Set(key, val)
		if err != nil {
			merr = append(merr, ReplicaError{Replica: i, Err: err})
		}
	}

	if len(merr) > s.MaxFailures {
		return merr
	}

	return nil
}

func (s *Storage) Delete(key string) (bool, error) {
	var merr MultiError
	var ok bool
	for i := 0; i < len(s.Replicas); i++ {
		o, err := s.Replicas[i].Delete(key)

		ok = ok || o

		if err != nil {
			merr = append(merr, ReplicaError{Replica: i, Err: err})
		}
	}

	if len(merr) > s.MaxFailures {
		return ok, merr
	}

	return ok, nil
}

func (s *Storage) ResetConnection(key string) error {
	var merr MultiError
	for i := 0; i < len(s.Replicas); i++ {
		err := s.Replicas[i].ResetConnection(key)
		if err != nil {
			merr = append(merr, ReplicaError{Replica: i, Err: err})
		}
	}

	if len(merr) > s.MaxFailures {
		return merr
	}

	return nil
}
