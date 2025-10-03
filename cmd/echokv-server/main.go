package main

import (
	"github.com/vantutran2k1/echokv/internal/server"
	"github.com/vantutran2k1/echokv/internal/store"
	"log"
)

func main() {
	s := store.NewStore()
	srv := server.NewServer(":5379", s)

	log.Fatal(srv.Start())
}
