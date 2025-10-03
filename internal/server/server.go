package server

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/vantutran2k1/echokv/internal/protocol"
	"github.com/vantutran2k1/echokv/internal/store"
)

type Server struct {
	listenAddr string
	store      *store.Store
}

func NewServer(listenAddr string, store *store.Store) *Server {
	return &Server{listenAddr: listenAddr, store: store}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	log.Printf("server started and listening on %s", s.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
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
		cmd, err := protocol.ParseCommand(line)
		if err != nil {
			if _, writeErr := conn.Write(fmt.Appendf(nil, "(error) %s\n", err)); writeErr != nil {
				log.Printf("error writing to connection: %v", writeErr)
			}
			continue
		}

		if cmd == nil {
			continue
		}

		s.executeCommand(conn, cmd, line)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("error reading from client %s: %v", conn.RemoteAddr(), err)
	}

	log.Printf("connection closed for: %s", conn.RemoteAddr())
}

func (s *Server) executeCommand(conn net.Conn, cmd *protocol.Command, rawCommand []byte) {
	var response []byte
	switch cmd.Name {
	case "GET":
		value, ok := s.store.Get(cmd.Key)
		if !ok {
			response = []byte("\n")
		} else {
			response = fmt.Appendf(nil, "%s\n", value)
		}
	case "SET":
		s.store.Set(cmd.Key, cmd.Value, rawCommand)
		response = []byte("OK\n")
	case "DELETE":
		s.store.Delete(cmd.Key, rawCommand)
		response = []byte("OK\n")
	default:
		response = fmt.Appendf(nil, "(error): unknown command '%s'\n", cmd.Name)
	}

	if _, err := conn.Write(response); err != nil {
		log.Printf("error writing to client %s: %v", conn.RemoteAddr(), err)
	}
}
