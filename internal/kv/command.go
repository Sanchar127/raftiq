package kv

import "encoding/json"

type CommandType string

const (
	CommandPut         CommandType = "PUT"
	CommandDelete      CommandType = "DELETE"
	CommandReadBarrier CommandType = "READ_BARRIER"
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`
}

func EncodeCommand(command Command) ([]byte, error) {
	return json.Marshal(command)
}

func DecodeCommand(data []byte) (Command, error) {
	var command Command

	if err := json.Unmarshal(data, &command); err != nil {
		return Command{}, err
	}

	return command, nil
}
