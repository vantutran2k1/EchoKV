package store

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestStoreSetGetDelete(t *testing.T) {
	s, cleanup := setupStore(t)
	defer cleanup()

	key := "test_key"
	value := "test_value"

	setRawCmd := fmt.Appendf(nil, "SET %s %s", key, value)
	deleteRawCmd := fmt.Appendf(nil, "DELETE %s", key)

	_, ok := s.Get(key)
	if ok {
		t.Errorf("key %s should not exist", key)
	}

	if err := s.Set(key, value, setRawCmd); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	retrievedValue, ok := s.Get(key)
	if !ok {
		t.Errorf("key %s should exist", key)
	}
	if retrievedValue != value {
		t.Errorf("retrieved value should be %s, got %s", value, retrievedValue)
	}

	if err := s.Delete(key, deleteRawCmd); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, ok = s.Get(key)
	if ok {
		t.Errorf("key %s should not exist", key)
	}
}

func TestStoreConcurrency(t *testing.T) {
	s, cleanup := setupStore(t)
	defer cleanup()

	key := "concurrency_key"
	value := "value"
	iterations := 1000

	var wg sync.WaitGroup
	wg.Add(iterations)

	rawCmd := []byte("SET concurrency_key final_value")
	for i := 0; i < iterations; i++ {
		go func(i int) {
			defer wg.Done()

			if i%2 == 0 {
				s.Set(key, value, rawCmd)
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
	if finalValue != value {
		t.Errorf("finalValue should be '%s', got '%s'", value, finalValue)
	}
}

func setupStore(t *testing.T) (*Store, func()) {
	tempDir, err := os.MkdirTemp("", "test-store-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	s, err := NewStore(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to initialize store: %v", err)
	}

	cleanup := func() {
		if err := s.Close(); err != nil {
			t.Errorf("error closing store: %v", err)
		}
		if err := os.RemoveAll(tempDir); err != nil {
			t.Errorf("error cleaning up temp dir %s: %v", tempDir, err)
		}
	}

	return s, cleanup
}
