package store

import (
	"sync"
)

const AOF_FILE_NAME = "echokv.aof"

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore(dir string) (*Store, error) {
	s := &Store{
		data: make(map[string]string),
	}

	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *Store) ApplySet(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(key, value)
}

func (s *Store) ApplyDelete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delete(key)
}

func (s *Store) set(key, value string) {
	s.data[key] = value
}

func (s *Store) delete(key string) {
	delete(s.data, key)
}
