package raftnode

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vantutran2k1/echokv/internal/store"
)

type Config struct {
	NodeID     string
	ListenAddr string
	RaftAddr   string
	JoinAddr   string
	DataDir    string
}

type Node struct {
	config *Config
	raft   *raft.Raft
	store  *store.Store
	server *net.Listener

	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
}

func NewNode(cfg *Config, store *store.Store) *Node {
	return &Node{
		config: cfg,
		store:  store,
	}
}

func (n *Node) Start() error {
	if err := n.setupRaftStorage(); err != nil {
		return err
	}

	serverListener, err := net.Listen("tcp", n.config.ListenAddr)
	if err != nil {
		return err
	}
	n.server = &serverListener
	log.Printf("Node %s: Client server listening on %s", n.config.NodeID, n.config.ListenAddr)

	return nil
}

func (n *Node) Shutdown() {
	if n.server != nil {
		(*n.server).Close()
	}

	if n.raft != nil {
		future := n.raft.Shutdown()
		if err := future.Error(); err != nil {
			log.Printf("node %s: warning: raft shutdown failed: %v", n.config.NodeID, err)
		} else {
			log.Printf("node %s: raft successfully shut down.", n.config.NodeID)
		}
	}
}

func (n *Node) setupRaftStorage() error {
	boltDBPath := filepath.Join(n.config.DataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return fmt.Errorf("failed to create bolt store at %s: %w", boltDBPath, err)
	}

	n.logStore = boltStore
	n.stableStore = boltStore

	snapshotStore, err := raft.NewFileSnapshotStore(n.config.DataDir, 1, os.Stdout)
	if err != nil {
		return fmt.Errorf("failed to c reate file snapshot store: %w", err)
	}
	n.snapshotStore = snapshotStore

	log.Printf("node %s: raft storage initialized in %s", n.config.NodeID, boltDBPath)

	return nil
}
