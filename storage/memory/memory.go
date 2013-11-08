package memory

import (
	"sync"
)

type Storage struct {
	store map[string][]byte
	mu    sync.Mutex
}

func New() *Storage {
	return &Storage{
		store: make(map[string][]byte),
	}
}

func (s *Storage) Get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.store[key]
	return val, ok, nil
}

func (s *Storage) Set(key string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[key] = val

	return nil
}

func (s *Storage) Delete(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// we do the lookup first so we can return whether or not we deleted a key
	// we've locked the map, so this is safe if a bit more expensive
	_, ok := s.store[key]
	if ok {
		delete(s.store, key)
	}

	return ok, nil
}

func (s *Storage) ResetConnection(key string) error {
	return nil
}
