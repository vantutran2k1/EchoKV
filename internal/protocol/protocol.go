package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrWrongArgNum    = errors.New("wrong number of arguments")
)

type Command struct {
	Name  string
	Key   string
	Value string
	Args  [][]byte
}

func ParseCommand(raw []byte) (*Command, error) {
	parts := bytes.Fields(raw)
	if len(parts) == 0 {
		return nil, nil
	}

	cmdName := strings.ToUpper(string(parts[0]))
	cmd := &Command{
		Name: cmdName,
		Args: parts[1:],
	}

	switch cmdName {
	case "SET":
		if len(parts) != 3 {
			return nil, fmt.Errorf("%w for SET command", ErrWrongArgNum)
		}

		cmd.Key = string(parts[1])
		cmd.Value = string(parts[2])
	case "GET", "DELETE":
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w for %s command", ErrWrongArgNum, cmdName)
		}

		cmd.Key = string(parts[1])
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownCommand, cmdName)
	}

	return cmd, nil
}
