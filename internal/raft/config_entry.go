package raft

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

const (
	configurationEntryMagic          uint32 = 0x52434647 // "RCFG"
	configurationEntryVersion        byte   = 1
	configurationEntryTypeStable     byte   = 1
	configurationEntryTypeEnterJoint byte   = 2
	configurationEntryTypeLeaveJoint byte   = 3

	configurationEntryHeaderSize = 4 + 1 + 1 + 4

	maxConfigurationVoters  = 1024
	maxConfigurationNodeID  = 1024
	maxConfigurationPayload = 1 << 20
)

type configurationEntry struct {
	EntryType byte
	Old       model.Configuration
	New       model.Configuration
}

// IsConfigurationEntry reports whether data contains the configuration-entry
// envelope used by Raft membership changes.
func IsConfigurationEntry(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	return binary.BigEndian.Uint32(data[:4]) == configurationEntryMagic
}

func validateConfiguration(configuration model.Configuration) error {
	if len(configuration.Voters) == 0 {
		return errors.New("configuration must contain at least one voter")
	}

	if len(configuration.Voters) > maxConfigurationVoters {
		return fmt.Errorf(
			"configuration contains too many voters: %d",
			len(configuration.Voters),
		)
	}

	seen := make(map[model.NodeID]struct{}, len(configuration.Voters))

	for _, voterID := range configuration.Voters {
		if voterID == "" {
			return errors.New("configuration contains empty voter ID")
		}

		if len(voterID) > maxConfigurationNodeID {
			return fmt.Errorf(
				"voter ID %q exceeds maximum length",
				voterID,
			)
		}

		if _, exists := seen[voterID]; exists {
			return fmt.Errorf(
				"configuration contains duplicate voter ID %q",
				voterID,
			)
		}

		seen[voterID] = struct{}{}
	}

	return nil
}
