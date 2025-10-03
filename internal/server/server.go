package server

import (
	"bufio"
	"io"
	"log"
	"net"
)

type Server struct {
	listenAddr string
}

func NewServer(listenAddr string) *Server {
	return &Server{listenAddr: listenAddr}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	log.Printf("Server started and listening on %s", s.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("Accepted new connection from: %s", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		log.Printf("Received from %s: %s", conn.RemoteAddr(), string(line))

		if _, err := conn.Write(append(line, '\n')); err != nil {
			if err != io.EOF {
				log.Printf("Error writing to client %s: %v", conn.RemoteAddr(), err)
			}
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from client %s: %v", conn.RemoteAddr(), err)
	}

	log.Printf("Connection closed for: %s", conn.RemoteAddr())
}
