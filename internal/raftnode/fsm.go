package raftnode

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/raft"
	"github.com/vantutran2k1/echokv/internal/store"
)

type fsm struct {
	*store.Store
}

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (f *fsm) Apply(l *raft.Log) any {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		log.Printf("fsm error: failed to unmarshal command: %v", err)
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch c.Op {
	case "SET":
		f.ApplySet(c.Key, c.Value)
		return nil
	case "DELETE":
		f.ApplyDelete(c.Key)
		return nil
	default:
		return fmt.Errorf("unsupported command op: %s", c.Op)
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.Store}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	restoredData := make(map[string]string)
	if err := json.NewDecoder(r).Decode(&restoredData); err != nil {
		return fmt.Errorf("failed to decode snapshot data: %w", err)
	}

	f.Store.RestoreData(restoredData)

	return nil
}

type fsmSnapshot struct {
	store *store.Store
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data := s.store.GetAllData()

	err := json.NewEncoder(sink).Encode(data)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to encode snapshot data: %w", err)
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
