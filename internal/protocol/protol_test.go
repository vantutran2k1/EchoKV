package protocol

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseCommand(t *testing.T) {
	testCases := []struct {
		name        string
		input       []byte
		expectedCmd *Command
		expectedErr error
		expectNil   bool
	}{
		{
			name:  "Valid SET command",
			input: []byte("SET key1 value1"),
			expectedCmd: &Command{
				Name:  "SET",
				Key:   "key1",
				Value: "value1",
				Args:  [][]byte{[]byte("key1"), []byte("value1")},
			},
		},
		{
			name:  "Valid GET command",
			input: []byte("GET key1"),
			expectedCmd: &Command{
				Name: "GET",
				Key:  "key1",
				Args: [][]byte{[]byte("key1")},
			},
		},
		{
			name:  "Valid DELETE command",
			input: []byte("delete key1"),
			expectedCmd: &Command{
				Name: "DELETE",
				Key:  "key1",
				Args: [][]byte{[]byte("key1")},
			},
		},
		{
			name:        "SET with wrong number of args",
			input:       []byte("SET key1"),
			expectedErr: ErrWrongArgNum,
		},
		{
			name:        "GET with wrong number of args",
			input:       []byte("GET key1 value1"),
			expectedErr: ErrWrongArgNum,
		},
		{
			name:        "Unknown command",
			input:       []byte("UPDATE key1 value1"),
			expectedErr: ErrUnknownCommand,
		},
		{
			name:      "Empty input line",
			input:     []byte(""),
			expectNil: true,
		},
		{
			name:      "Input with only whitespace",
			input:     []byte("   "),
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := ParseCommand(tc.input)

			if tc.expectedErr != nil {
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("expected: %v, got: %v", tc.expectedErr, err)
				}
				return
			}
			if err != nil {
				t.Errorf("did not expect error, but got: %v", err)
			}

			if tc.expectNil {
				if cmd != nil {
					t.Errorf("expected nil, but got: %v", cmd)
				}
				return
			}

			if !reflect.DeepEqual(cmd, tc.expectedCmd) {
				t.Errorf("expected: %v, got: %v", tc.expectedCmd, cmd)
			}
		})
	}
}
