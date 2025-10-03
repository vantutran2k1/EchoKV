package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vantutran2k1/echokv/internal/server"
	"github.com/vantutran2k1/echokv/internal/store"
)

const (
	listenAddr = ":5379"
	dataDir    = "data"
)

func main() {
	if err := os.MkdirAll(dataDir, 0775); err != nil {
		log.Fatalf("failed to create data directory %s: %v", dataDir, err)
	}

	s, err := store.NewStore(dataDir)
	if err != nil {
		log.Fatalf("failed to initialize store: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	cleanup := func() {
		log.Println("shutting down server...")
		if closeErr := s.Close(); closeErr != nil {
			log.Printf("warning: failed to gracefully close store: %v", err)
		}
		log.Println("server shut down completed")
	}
	defer cleanup()

	srv := server.NewServer(listenAddr, s)
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("server failed to start: %v", err)
		}
	}()

	log.Printf("started server on %s with data in %s", listenAddr, dataDir)

	<-quit
}
