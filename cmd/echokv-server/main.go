package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vantutran2k1/echokv/internal/raftnode"
	"github.com/vantutran2k1/echokv/internal/store"
)

const (
	listenAddr = ":5379"
	dataDir    = "data"
)

func main() {
	var cfg raftnode.Config

	flag.StringVar(&cfg.NodeID, "node-id", "node-1", "A unique ID for the node.")
	flag.StringVar(&cfg.ListenAddr, "listen-addr", ":5379", "Address for client connections.")
	flag.StringVar(&cfg.RaftAddr, "raft-addr", ":15379", "Address for inter-node Raft communication.")
	flag.StringVar(&cfg.JoinAddr, "join-addr", "", "Address of an existing node to join the cluster.")
	flag.StringVar(&cfg.DataDir, "data-dir", "data", "Directory to store Raft logs, snapshots, and AOF file.")

	flag.Parse()

	cfg.DataDir = cfg.DataDir + "/" + cfg.NodeID

	if err := os.MkdirAll(cfg.DataDir, 0775); err != nil {
		log.Fatalf("failed to create data directory %s: %v", cfg.DataDir, err)
	}

	s, err := store.NewStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("failed to initialize store: %v", err)
	}

	node := raftnode.NewNode(&cfg, s)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	cleanup := func() {
		log.Println("shutting down server...")
		node.Shutdown()
		if closeErr := s.Close(); closeErr != nil {
			log.Printf("warning: failed to gracefully close store: %v", err)
		}
		log.Println("server shut down completed")
	}
	defer cleanup()

	log.Printf("Starting echo-kv node [%s]...", cfg.NodeID)
	if err := node.Start(); err != nil {
		log.Fatalf("node failed to start: %v", err)
	}

	<-quit
}
