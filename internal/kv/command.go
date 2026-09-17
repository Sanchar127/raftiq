package kv

import (
	"encoding/json"

	"github.com/sanchar127/raftiq/internal/model"
)

// CommandType identifies the operation encoded in a Raft command.
type CommandType string

// Command types supported by the KV state machine.
const (
	CommandPut         CommandType = "PUT"
	CommandDelete      CommandType = "DELETE"
	CommandReadBarrier CommandType = "READ_BARRIER"

	CommandLockAcquire CommandType = "LOCK_ACQUIRE"
	CommandLockExpire  CommandType = "LOCK_EXPIRE"
	CommandFencedPut   CommandType = "FENCED_PUT"

	CommandCreateJob    CommandType = "CREATE_JOB"
	CommandClaimJob     CommandType = "CLAIM_JOB"
	CommandJobReclaim   CommandType = "JOB_RECLAIM"
	CommandJobStart     CommandType = "JOB_START"
	CommandJobSucceeded CommandType = "JOB_SUCCEEDED"
	CommandJobFailed    CommandType = "JOB_FAILED"
)

// Command represents an operation encoded into a Raft log entry.
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`

	JobID       string `json:"job_id,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	ScheduledAt int64  `json:"scheduled_at,omitempty"`

	OwnerID       string         `json:"owner_id,omitempty"`
	ExpiresAt     int64          `json:"expires_at,omitempty"`
	FencingToken  uint64         `json:"fencing_token,omitempty"`
	ExecutionID   string         `json:"execution_id,omitempty"`
	ExpectedState model.JobState `json:"expected_state,omitempty"`
	At            int64          `json:"at,omitempty"`
}

// EncodeCommand serializes a command into JSON.
func EncodeCommand(command Command) ([]byte, error) {
	return json.Marshal(command)
}

// DecodeCommand deserializes a JSON-encoded command.
func DecodeCommand(data []byte) (Command, error) {
	var command Command

	if err := json.Unmarshal(data, &command); err != nil {
		return Command{}, err
	}

	return command, nil
}
