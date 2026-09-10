package kv

import "encoding/json"

type CommandType string

const (
	CommandPut         CommandType = "PUT"
	CommandDelete      CommandType = "DELETE"
	CommandReadBarrier CommandType = "READ_BARRIER"

	CommandLockAcquire CommandType = "LOCK_ACQUIRE"
	CommandLockExpire  CommandType = "LOCK_EXPIRE"
	CommandFencedPut   CommandType = "FENCED_PUT"

	CommandClaimJob CommandType = "CLAIM_JOB"
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`

	JobID string `json:"job_id,omitempty"`

	OwnerID      string `json:"owner_id,omitempty"`
	ExpiresAt    int64  `json:"expires_at,omitempty"`
	FencingToken uint64 `json:"fencing_token,omitempty"`
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
