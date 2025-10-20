package raftnode

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vantutran2k1/echokv/internal/protocol"
	"github.com/vantutran2k1/echokv/internal/store"
)

const MaxJoinAttempts = 3
const JoinRetryInterval = 5 * time.Second

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

	logger *slog.Logger
}

func NewNode(cfg *Config, store *store.Store) *Node {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With(
		slog.String("node_id", cfg.NodeID),
		slog.String("data_dir", cfg.DataDir),
	)

	return &Node{
		config: cfg,
		store:  store,
		logger: logger,
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

	n.logger.Info("client server listening", slog.String("addr", n.config.ListenAddr))

	return nil
}

func (n *Node) Shutdown() {
	if n.server != nil {
		(*n.server).Close()
	}

	if n.raft != nil {
		future := n.raft.Shutdown()
		if err := future.Error(); err != nil {
			n.logger.Warn("raft shutdown failed", slog.String("error", err.Error()))
		} else {
			n.logger.Info("raft successfully shut down")
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

	n.logger.Info("received request to join from %s at %s", nodeID, raftAddr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			n.logger.Info("already a member, skipping join", slog.String("node", nodeID))
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

	n.logger.Info("successfully added new voter %s at %s", nodeID, raftAddr)
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

	n.logger.Info("raft storage initialized", slog.String("path", boltDBPath))

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
		return fmt.Errorf("failed to read last index from log store: %w", err)
	}

	if lastIndex == 0 {
		if n.config.JoinAddr == "" {
			n.logger.Info("bootstrapping new cluster...")
			if err := n.bootstrapNewCluster(raftCfg, transport); err != nil {
				return err
			}
		} else {
			n.logger.Info("starting active join retry mechanism", slog.String("join_target", n.config.JoinAddr), slog.Int("max_attempts", MaxJoinAttempts))
			go n.retryJoinCluster()
		}
	} else {
		n.logger.Info("resuming raft operations from existing state", slog.Int("last index", int(lastIndex)))
	}

	return nil
}

func (n *Node) retryJoinCluster() {
	ticker := time.NewTicker(JoinRetryInterval)
	defer ticker.Stop()

	attempts := 0
	for range ticker.C {
		if n.raft.State() != raft.Follower {
			n.logger.Info("node is no longer unconfigured, stopping join attempts")
			return
		}

		attempts++
		if attempts > MaxJoinAttempts {
			n.logger.Error("exceeded max join attempts, shutting down automatic join process", slog.Int("attempts", MaxJoinAttempts))
			return
		}

		n.logger.Debug("attempting to join cluster", slog.Int("attempt", attempts))
		n.doJoinCluster()
	}
}

func (n *Node) doJoinCluster() {
	lastIndex, _ := n.logStore.LastIndex()
	if lastIndex > 0 {
		return
	}

	n.logger.Debug("actively attempting to join cluster", slog.String("target_addr", n.config.JoinAddr))

	conn, err := net.Dial("tcp", n.config.JoinAddr)
	if err != nil {
		n.logger.Warn("failed to connect to join address", slog.Any("error", err))
		return
	}
	defer conn.Close()

	command := fmt.Sprintf("JOIN %s %s\n", n.config.NodeID, n.config.RaftAddr)
	if _, err := conn.Write([]byte(command)); err != nil {
		n.logger.Error("failed to send JOIN command", slog.Any("error", err))
		return
	}

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		response := scanner.Text()
		if strings.HasPrefix(strings.ToLower(response), "ok") {
			n.logger.Info("successfully joined cluster!", slog.String("response", response))
			return
		}
		n.logger.Warn("JOIN command rejected by leader", slog.String("response", response))
	}
}

func (n *Node) bootstrapNewCluster(raftCfg *raft.Config, transport *raft.NetworkTransport) error {
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
		n.logger.Info("cluster already bootstrapped by another node")
	}

	return nil
}

func (n *Node) handleClientConnections() {
	for {
		conn, err := (*n.server).Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Temporary() {
				n.logger.Error("temporary error accepting connection", slog.String("error", err.Error()))
				continue
			}
			n.logger.Error("listener closed or permanent error", slog.String("error", err.Error()))
			return
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()
	n.logger.Info("accepted new connection", slog.String("addr", conn.RemoteAddr().String()))

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
			response = []byte("ok\n")
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
			response = []byte("ok\n")
		}
	default:
		response = fmt.Appendf(nil, "-error unknown command '%s'\n", cmd.Name)
	}

	if _, err := conn.Write(response); err != nil {
		n.logger.Error("error writing to client", slog.String("addr", conn.RemoteAddr().String()), slog.String("error", err.Error()))
	}
}
