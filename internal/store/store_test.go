package store

import (
	"sync"
	"testing"
)

func TestStoreSetGetDelete(t *testing.T) {
	s := NewStore()
	key := "test_key"
	value := "test_value"

	_, ok := s.Get(key)
	if ok {
		t.Errorf("key %s should not exist", key)
	}

	s.Set(key, value)
	retrievedValue, ok := s.Get(key)
	if !ok {
		t.Errorf("key %s should exist", key)
	}
	if retrievedValue != value {
		t.Errorf("retrieved value should be %s, got %s", value, retrievedValue)
	}

	s.Delete(key)
	_, ok = s.Get(key)
	if ok {
		t.Errorf("key %s should not exist", key)
	}
}

func TestStoreConcurrency(t *testing.T) {
	s := NewStore()
	key := "concurrency_key"
	iterations := 1000

	var wg sync.WaitGroup
	wg.Add(iterations)

	for i := 0; i < iterations; i++ {
		go func(i int) {
			defer wg.Done()

			if i%2 == 0 {
				s.Set(key, "value")
			} else {
				s.Get(key)
			}
		}(i)
	}

	wg.Wait()

	finalValue, ok := s.Get(key)
	if !ok {
		t.Errorf("key %s should exist", key)
	}
	if finalValue != "value" {
		t.Errorf("finalValue should be '%s', got '%s'", "value", finalValue)
	}
}
