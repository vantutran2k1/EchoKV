package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/vantutran2k1/echokv/internal/protocol"
)

func main() {
	var connectAddr string
	flag.StringVar(&connectAddr, "connect", "127.0.0.1:5379", "The address of the EchoKV server node to connect to")
	flag.Parse()

	log.Printf("connecting to %s...", connectAddr)

	conn, err := net.Dial("tcp", connectAddr)
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()
	log.Printf("connected, enter commands (%s, %s, %s, %s, %s)", protocol.OP_GET, protocol.OP_SET, protocol.OP_DELETE, protocol.OP_JOIN, protocol.OP_REMOVE)

	reader := bufio.NewReader(os.Stdin)
	serverScanner := bufio.NewScanner(conn)

	responseChan := make(chan string)
	errorChan := make(chan error)

	go func() {
		for serverScanner.Scan() {
			responseChan <- serverScanner.Text()
		}
		if err := serverScanner.Err(); err != nil {
			errorChan <- fmt.Errorf("error reading server response: %w", err)
		}

		close(responseChan)
	}()

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, os.ErrClosed) || err.Error() == "EOF" {
				break
			}
			log.Printf("error reading input: %v", err)
			continue
		}

		command := strings.TrimSpace(input)
		if _, err := conn.Write([]byte(command + "\n")); err != nil {
			log.Fatalf("failed to send command: %v", err)
		}

		select {
		case response, ok := <-responseChan:
			if !ok {
				log.Println("server closed the connection")
				return
			}
			fmt.Println(response)
		case err := <-errorChan:
			log.Fatalf("fatal communication error: %v", err)
		}
	}
}
