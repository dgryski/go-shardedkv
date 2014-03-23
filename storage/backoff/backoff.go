// Package backoff implements a fail-fast backoff strategy for failing Storage backends.
/*

This package wraps a storage backend and, when the underlying storage fails,
allows it to fail "a few times" before causing it to *immediately* fail until
the backoff period has timed out.  It will then allow API calls to the storage
engine again.  More fails will cause the backoff delay to grow, while a success
resets the failure counter.

This is useful in conjunction with the Replica storage backend so that failed
replicas will fail immediately instead of causing API calls to take excessively
long due to connect timeouts etc.

*/
package backoff

import (
	"errors"
	"time"

	"github.com/dgryski/go-shardedkv"
)

type storageState int

const (
	stateOK storageState = iota
	stateWarn
	stateFail
	stateRetry
)

// The default maximum delay between retries
const DefaultMaxDelay = 60

// Storage is a storage backend that tracks storage engine failures.
type Storage struct {
	// The underlying storage backend
	Store shardedkv.Storage
	// The number of failures allowed until the storage is marked as failed.
	MaxWarns int
	// The maximum backoff time in seconds.  Default 60 seconds.
	MaxDelay int

	state     storageState
	fails     int
	delay     int
	skipUntil time.Time
}

// ErrBackingOff is the error returned if the package determines a storage backend is not currently suitable for use.
var ErrBackingOff = errors.New("backing off")

// for mocking during testing
var timeNow = time.Now

func (s *Storage) canUse() error {

	switch s.state {

	case stateOK, stateWarn:
		return nil

	case stateRetry:
		// panic("bad state transition")

	case stateFail:
		if timeNow().Before(s.skipUntil) {
			return ErrBackingOff
		}

		s.state = stateRetry
		return nil

	default:
		// panic("bad transition")
	}

	return nil

}

func (s *Storage) fail() {

	switch s.state {

	case stateOK:
		s.state = stateWarn
		s.fails = 1

	case stateWarn:
		s.fails++
		if s.MaxWarns == 0 || (s.MaxWarns >= 0 && s.fails == s.MaxWarns) {
			s.state = stateFail
			s.delay = 1
			// FIXME(dgryski): possible to mock this for testing?
			s.skipUntil = timeNow().Add(time.Duration(s.delay) * time.Second)
		}

	case stateFail:
		// panic("bad state transition")

	case stateRetry:
		s.state = stateFail
		s.delay *= 2
		maxDelay := s.MaxDelay
		if maxDelay == 0 {
			maxDelay = DefaultMaxDelay
		}
		if s.delay >= maxDelay {
			s.delay = maxDelay
		}
		s.skipUntil = timeNow().Add(time.Duration(s.delay) * time.Second)

	default:
		// panic("unknown state")
	}
}

func (s *Storage) success() {
	s.state = stateOK
	s.fails = 0
	s.delay = 0
	s.skipUntil = time.Time{}
}

// Get implements the shardedkv.Storage interface
func (s *Storage) Get(key string) ([]byte, bool, error) {

	err := s.canUse()

	if err != nil {
		return nil, false, err
	}

	val, ok, err := s.Store.Get(key)

	if err == nil {
		s.success()
	} else {
		s.fail()
	}

	return val, ok, err
}

// Set implements the shardedkv.Storage interface
func (s *Storage) Set(key string, value []byte) error {

	err := s.canUse()

	if err != nil {
		return err
	}

	err = s.Store.Set(key, value)

	if err == nil {
		s.success()
	} else {
		s.fail()
	}

	return err
}

// Delete implements the shardedkv.Storage interface
func (s *Storage) Delete(key string) (bool, error) {

	err := s.canUse()

	if err != nil {
		return false, err
	}

	ok, err := s.Store.Delete(key)

	if err == nil {
		s.success()
	} else {
		s.fail()
	}

	return ok, err
}

// ResetConnection implements the shardedkv.Storage interface
func (s *Storage) ResetConnection(key string) error {

	err := s.Store.ResetConnection(key)

	// reset the failure counters
	// assume after resetting everything will work
	s.success()

	return err
}
