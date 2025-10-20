package raftnode

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/vantutran2k1/echokv/internal/store"
)

type TestHarness struct {
	t     *testing.T
	nodes []*Node
	mu    sync.Mutex
	dir   string
}

func NewTestHarness(t *testing.T) *TestHarness {
	tempDir, err := os.MkdirTemp("", "echokv-raft-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Logf("test harness using directory: %s", tempDir)

	return &TestHarness{
		t:   t,
		dir: tempDir,
	}
}

func TestFaultTolerance(t *testing.T) {
	h := NewTestHarness(t)
	defer h.cleanUp()

	p1Client, _ := getFreePort()
	p1Raft, _ := getFreePort()
	p2Client, _ := getFreePort()
	p2Raft, _ := getFreePort()
	p3Client, _ := getFreePort()
	p3Raft, _ := getFreePort()

	joinAddr := "127.0.0.1:" + strconv.Itoa(p1Client)

	t.Logf("ports: N1=%d/%d, N2=%d/%d, N3=%d/%d", p1Client, p1Raft, p2Client, p2Raft, p3Client, p3Raft)

	node1 := h.AddNode("node-1", p1Client, p1Raft, "")
	node2 := h.AddNode("node-2", p2Client, p2Raft, joinAddr)
	node3 := h.AddNode("node-3", p3Client, p3Raft, joinAddr)

	h.waitForCluster(3)

	initialLeader := h.waitForLeader()
	if initialLeader == nil {
		t.Fatal("initial leader election failed")
	}
	t.Logf("initial leader is: %s", initialLeader.config.NodeID)

	key, value := "version", "p2_final"
	conn := h.getClientConn(initialLeader)

	if _, err := conn.Write(fmt.Appendf(nil, "SET %s %s\n", key, value)); err != nil {
		t.Fatalf("failed to send SET command: %v", err)
	}

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() || scanner.Text() != "ok" {
		t.Fatalf("write failed or timed out, got: %s", scanner.Text())
	}
	conn.Close()

	initialLeaderAppliedIndex := initialLeader.raft.AppliedIndex()
	h.waitForAppliedIndex(node2, initialLeaderAppliedIndex)

	conn = h.getClientConn(node2)
	if _, err := conn.Write(fmt.Appendf(nil, "GET %s\n", key)); err != nil {
		t.Fatalf("failed to send GET command: %v", err)
	}
	scanner = bufio.NewScanner(conn)
	if !scanner.Scan() || scanner.Text() != value {
		t.Fatalf("follower replication failed, got: %s", scanner.Text())
	}
	conn.Close()

	// --- Failure Injection
	t.Logf("injecting failure: shutting down initial leader %s", initialLeader.config.NodeID)
	initialLeader.Shutdown()

	h.nodes = h.nodes[:0]
	for _, node := range []*Node{node1, node2, node3} {
		if node != initialLeader {
			h.nodes = append(h.nodes, node)
		}
	}

	newLeader := h.waitForLeader()
	if newLeader == nil {
		t.Fatal("failed to elect a new leader after crash")
	}
	t.Logf("new leader elected: %s", newLeader.config.NodeID)

	conn = h.getClientConn(newLeader)
	if _, err := conn.Write([]byte(fmt.Sprintf("GET %s\n", key))); err != nil {
		t.Fatalf("failed to send GET command to new leader:  %v", err)
	}

	scanner = bufio.NewScanner(conn)
	if !scanner.Scan() || scanner.Text() != value {
		t.Fatalf("data loss, new leader could not retrieve committed data, got: %s", scanner.Text())
	}
	conn.Close()
}

func (h *TestHarness) AddNode(nodeID string, listenPort int, raftPort int, joinAddr string) *Node {
	h.mu.Lock()
	defer h.mu.Unlock()

	cfg := &Config{
		NodeID:     nodeID,
		ListenAddr: ":" + strconv.Itoa(listenPort),
		RaftAddr:   "127.0.0.1:" + strconv.Itoa(raftPort),
		JoinAddr:   joinAddr,
		DataDir:    filepath.Join(h.dir, nodeID),
	}

	if err := os.MkdirAll(cfg.DataDir, 0775); err != nil {
		h.t.Fatalf("failed to create node dir: %v", err)
	}

	s, err := store.NewStore(cfg.DataDir)
	if err != nil {
		h.t.Fatalf("failed to initialize store: %v", err)
	}

	node := NewNode(cfg, s)
	if err := node.Start(); err != nil {
		h.t.Fatalf("failed to start node %s: %v", nodeID, err)
	}

	h.nodes = append(h.nodes, node)
	return node
}

func (h *TestHarness) waitForLeader() *Node {
	h.t.Helper()
	timeout := time.After(10 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			h.t.Fatalf("timed out waiting for leader election")
			return nil
		case <-tick.C:
			for _, node := range h.nodes {
				if node.raft != nil && node.raft.State() == raft.Leader {
					return node
				}
			}
		}
	}
}

func (h *TestHarness) getClientConn(node *Node) net.Conn {
	h.t.Helper()
	conn, err := net.Dial("tcp", node.config.ListenAddr)
	if err != nil {
		h.t.Fatalf("failed to connect to node %s at %s: %v", node.config.NodeID, node.config.ListenAddr, err)
	}
	return conn
}

func (h *TestHarness) cleanUp() {
	for _, node := range h.nodes {
		node.Shutdown()
	}
	if err := os.RemoveAll(h.dir); err != nil {
		log.Printf("warning: failed to remove temporary directory %s: %v", h.dir, err)
	}
}

func (h *TestHarness) waitForAppliedIndex(node *Node, index uint64) {
	h.t.Helper()
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(50 * time.Microsecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			h.t.Fatalf("timed out waiting for node %s to apply index %d", node.config.NodeID, index)
			return
		case <-tick.C:
			if node.raft != nil && node.raft.AppliedIndex() >= index {
				return
			}
		}
	}
}

func (h *TestHarness) waitForCluster(expectedSize int) {
	h.t.Helper()
	timeout := time.After(15 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	leader := h.waitForLeader()
	if leader == nil {
		h.t.Fatal("leader failed to elect before checking cluster size")
	}

	for {
		select {
		case <-timeout:
			h.t.Fatalf("timed out waiting for cluster size to reach %d", expectedSize)
			return
		case <-tick.C:
			future := leader.raft.GetConfiguration()
			if err := future.Error(); err == nil {
				config := future.Configuration()
				if len(config.Servers) == expectedSize {
					return
				}
			}
		}
	}
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	// Listening on port 0 asks the OS to choose an available port.
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
