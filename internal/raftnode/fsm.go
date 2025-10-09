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
	return nil, fmt.Errorf("snapshotting is not yet implemented (Phase 4)")
}

func (f *fsm) Restore(r io.ReadCloser) error {
	return fmt.Errorf("restore from snapshot is not yet implemented (Phase 4)")
}
