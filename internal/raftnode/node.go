package raftnode

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vantutran2k1/echokv/internal/protocol"
	"github.com/vantutran2k1/echokv/internal/store"
)

const RaftApplyTimeout = 500 * time.Millisecond

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

func (n *Node) ApplyCommand(c Command) (any, error) {
	if n.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not the leader")
	}

	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %w", err)
	}

	future := n.raft.Apply(b, RaftApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply failed: %w", err)
	}

	return future.Response(), nil
}

func (n *Node) JoinCluster(nodeID, raftAddr string) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("cannot process join: not hte leader")
	}

	log.Printf("node %s: received request to join from %s at %s", n.config.NodeID, nodeID, raftAddr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			log.Printf("node %s: node %s already a member, skipping join", n.config.NodeID, nodeID)
			return nil
		}
	}

	addFuture := n.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0,
		0,
	)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	log.Printf("node %s: successfully added new voter %s at %s", n.config.NodeID, nodeID, raftAddr)
	return nil
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
		log.Printf("node %s: resuming raft operations from existing state (last index: %d)", n.config.NodeID, lastIndex)
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
	defer conn.Close()
	log.Printf("node %s: accepted new connection from: %s", n.config.NodeID, conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		rawCommand := scanner.Bytes()
		cmd, err := protocol.ParseCommand(rawCommand)
		if err != nil {
			conn.Write(fmt.Appendf(nil, "(error) %s\n", err))
			continue
		}
		if cmd == nil {
			continue
		}

		n.executeCommand(conn, cmd)
	}
}

func (n *Node) executeCommand(conn net.Conn, cmd *protocol.Command) {
	var response []byte
	var err error

	switch cmd.Name {
	case "GET":
		value, ok := n.store.Get(cmd.Key)
		if !ok {
			response = []byte("(nil)\n")
		} else {
			response = fmt.Appendf(nil, "%s\n", value)
		}
	case "SET", "DELETE":
		if n.raft.State() != raft.Leader {
			_, leaderAddr := n.raft.LeaderWithID()
			if leaderAddr == "" {
				response = []byte("-error no leader currently elected\n")
			} else {
				// TODO: forwarding to leader node
				response = fmt.Appendf(nil, "-error not leader, leader address: %s\n", leaderAddr)
			}
			break
		}

		raftCmd := Command{Op: cmd.Name, Key: cmd.Key, Value: cmd.Value}

		_, err = n.ApplyCommand(raftCmd)
		if err != nil {
			response = fmt.Appendf(nil, "-error raft apply failed: %s\n", err)
		} else {
			response = []byte("OK\n")
		}
	case "JOIN":
		if len(cmd.Args) != 2 {
			response = []byte("-error wrong number of arguments for JOIN. Usage: JOIN <node-id> <raft-addr>\n")
			break
		}

		joiningNodeID := string(cmd.Args[0])
		joiningRaftAddr := string(cmd.Args[1])

		err = n.JoinCluster(joiningNodeID, joiningRaftAddr)
		if err != nil {
			response = fmt.Appendf(nil, "-error JOIN failed: %s\n", err)
		} else {
			response = []byte("OK\n")
		}
	default:
		response = fmt.Appendf(nil, "-error unknown command '%s'\n", cmd.Name)
	}

	if _, err := conn.Write(response); err != nil {
		log.Printf("node %s: error writing to client %s: %v", n.config.NodeID, conn.RemoteAddr(), err)
	}
}
