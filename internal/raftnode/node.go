package raftnode

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

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

	if err := n.setupRaftCore(); err != nil {
		return err
	}

	serverListener, err := net.Listen("tcp", n.config.ListenAddr)
	if err != nil {
		return err
	}
	n.server = &serverListener

	go n.handleClientConnections()

	log.Printf("node %s: client server listening on %s", n.config.NodeID, n.config.ListenAddr)

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

func (n *Node) setupRaftCore() error {
	addr, err := net.ResolveTCPAddr("tcp", n.config.RaftAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve raft addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(
		n.config.RaftAddr,
		addr,
		3,
		10*time.Second,
		os.Stdout,
	)
	if err != nil {
		return fmt.Errorf("failed to create raft tcp transport: %w", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(n.config.NodeID)

	ra, err := raft.NewRaft(
		raftCfg,
		&fsm{Store: n.store},
		n.logStore,
		n.stableStore,
		n.snapshotStore,
		transport,
	)
	if err != nil {
		return fmt.Errorf("failed to create new raft node: %w", err)
	}
	n.raft = ra

	lastIndex, err := n.logStore.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to read last index from lgo store: %w", err)
	}

	if lastIndex == 0 {
		if n.config.JoinAddr == "" {
			log.Printf("node %s: bootstrapping new cluster...", n.config.NodeID)

			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftCfg.LocalID,
						Address: transport.LocalAddr(),
					},
				},
			}

			future := n.raft.BootstrapCluster(configuration)
			if err := future.Error(); err != nil {
				if err != raft.ErrCantBootstrap {
					return fmt.Errorf("failed to bootstrap cluster: %w", err)
				}
				log.Printf("node %s: cluster already bootstrapped by another node", n.config.NodeID)
			}
		} else {
			log.Printf("node %s: started in join mode", n.config.NodeID)
		}
	} else {
		log.Panicf("node %s: resuming raft operations from existing state (last index: %d)", n.config.NodeID, lastIndex)
	}

	return nil
}

func (n *Node) handleClientConnections() {
	for {
		conn, err := (*n.server).Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Temporary() {
				log.Printf("node %s: temporary error accepting connection: %v", n.config.NodeID, err)
				continue
			}
			log.Printf("node %s: listener closed or permanent error: %v", n.config.NodeID, err)
			return
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	conn.Close()
}
