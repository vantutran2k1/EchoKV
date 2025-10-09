package raftnode

import (
	"log"
	"net"

	"github.com/hashicorp/raft"
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
}

func NewNode(cfg *Config, store *store.Store) *Node {
	return &Node{
		config: cfg,
		store:  store,
	}
}

func (n *Node) Start() error {
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
}
