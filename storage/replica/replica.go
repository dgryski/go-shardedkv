// Package replica provides a replicated storage backend
package replica

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	shardedkv "github.com/dgryski/go-shardedkv"
)

type Storage struct {
	MaxFailures int
	Replicas    []shardedkv.Storage
	hedgedTime  time.Duration
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
		hedgedTime:  1 * time.Second,
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

	ch := make(chan result, 2)
	go f(idx1, r1, ch)

	var r result
	var timedOut bool

	select {
	case <-time.After(s.hedgedTime):
		timedOut = true
	case r = <-ch:
		// got a response, we're done
	}

	if timedOut {
		// query the second replica and wait for a response from somebody
		go f(idx2, r2, ch)
		r = <-ch
	}

	// if we're here, r has been filled in either by the select loop or by
	// in the timedOut block above

	if r.ok && r.err == nil {
		// success -- return what we got
		return r.b, true, nil
	}

	// store this error away
	// note that r.err might actually be nil here
	rerr := ReplicaError{Replica: r.idx, Err: r.err}

	// try the other replica if we haven't already
	if !timedOut {
		go f(idx2, r2, ch)
	}
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

	errch := make(chan *ReplicaError)

	for i := 0; i < len(s.Replicas); i++ {
		go func(replica int, errch chan *ReplicaError) {
			err := s.Replicas[replica].Set(key, val)
			var reperr *ReplicaError
			if err != nil {
				reperr = &ReplicaError{Replica: replica, Err: err}
			}
			errch <- reperr
		}(i, errch)
	}

	var merr MultiError
	for i := 0; i < len(s.Replicas); i++ {
		reperr := <-errch
		if reperr != nil {
			merr = append(merr, *reperr)
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

// SetHedgedTimeout sets the timeout for a single replica to respond before a querying a backup.
func (s *Storage) SetHedgedTimeout(timeout time.Duration) { s.hedgedTime = timeout }

// HedgedTimeout returns the timeout for a single replica to respond before querying a backup.  Defaults to 1 second.
func (s *Storage) HedgedTimeout() time.Duration { return s.hedgedTime }
