package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/vantutran2k1/echokv/internal/protocol"
)

const AOF_FILE_NAME = "echokv.aof"

type Store struct {
	mu        sync.RWMutex
	data      map[string]string
	aofFile   *os.File
	aofWriter *bufio.Writer
	aofPath   string
}

func NewStore(dir string) (*Store, error) {
	s := &Store{
		data: make(map[string]string),
	}

	s.aofPath = strings.Join([]string{dir, AOF_FILE_NAME}, string(os.PathSeparator))

	if err := s.Restore(); err != nil {
		return nil, fmt.Errorf("failed to restore state: %w", err)
	}

	file, err := os.OpenFile(s.aofPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file %s for appending: %w", s.aofPath, err)
	}

	s.aofFile = file
	s.aofWriter = bufio.NewWriter(file)

	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.aofWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF writer: %w", err)
	}

	if err := s.aofFile.Close(); err != nil {
		return fmt.Errorf("failed to close AOF file: %w", err)
	}

	return nil
}

func (s *Store) Restore() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.aofPath, os.O_RDONLY, 0660)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to open AOF file for recovery: %W", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++

		rawCommand := scanner.Bytes()
		cmd, err := protocol.ParseCommand(rawCommand)
		if err != nil {
			return fmt.Errorf("error parsing command on AOF line %d: %w", lineNum, err)
		}
		if cmd == nil {
			continue
		}

		switch cmd.Name {
		case "SET":
			s.data[cmd.Key] = cmd.Value
		case "DELETE":
			delete(s.data, cmd.Key)
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("error reading AOF file: %w", err)
	}

	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Set(key, value string, rawCommand []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value

	return s.addLog(rawCommand)
}

func (s *Store) Delete(key string, rawCommand []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return nil
	}

	delete(s.data, key)

	return s.addLog(rawCommand)
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

func (s *Store) addLog(rawCommand []byte) error {
	if _, err := s.aofWriter.Write(rawCommand); err != nil {
		return fmt.Errorf("failed to write command to AOF: %w", err)
	}

	if _, err := s.aofWriter.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline to AOF: %w", err)
	}

	if err := s.aofWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF writer: %w", err)
	}

	if err := s.aofFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync AOF file to disk: %w", err)
	}

	return nil
}
