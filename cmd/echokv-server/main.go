package main

import (
	"github.com/vantutran2k1/echokv/internal/server"
	"log"
)

func main() {
	srv := server.NewServer(":5379")
	log.Fatal(srv.Start())
}
