package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

const (
	configurationEntryMagic   uint32 = 0x52434647 // "RCFG"
	configurationEntryVersion byte   = 1
	configurationEntryType    byte   = 1

	configurationEntryHeaderSize = 4 + 1 + 1 + 4

	maxConfigurationVoters  = 1024
	maxConfigurationNodeID  = 1024
	maxConfigurationPayload = 1 << 20
)

// EncodeConfigurationEntry encodes a stable Raft voter configuration
// into a dedicated Raft configuration-entry envelope.
func EncodeConfigurationEntry(configuration model.Configuration) ([]byte, error) {
	if err := validateConfiguration(configuration); err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, configurationEntryMagic); err != nil {
		return nil, fmt.Errorf("encode configuration magic: %w", err)
	}

	if err := buf.WriteByte(configurationEntryVersion); err != nil {
		return nil, fmt.Errorf("encode configuration version: %w", err)
	}

	if err := buf.WriteByte(configurationEntryType); err != nil {
		return nil, fmt.Errorf("encode configuration entry type: %w", err)
	}

	if err := binary.Write(
		&buf,
		binary.BigEndian,
		uint32(len(configuration.Voters)),
	); err != nil {
		return nil, fmt.Errorf("encode voter count: %w", err)
	}

	for _, voterID := range configuration.Voters {
		id := []byte(voterID)

		if err := binary.Write(
			&buf,
			binary.BigEndian,
			uint32(len(id)),
		); err != nil {
			return nil, fmt.Errorf("encode voter ID length: %w", err)
		}

		if _, err := buf.Write(id); err != nil {
			return nil, fmt.Errorf("encode voter ID: %w", err)
		}
	}

	if buf.Len() > maxConfigurationPayload {
		return nil, errors.New("configuration entry exceeds maximum payload size")
	}

	return buf.Bytes(), nil
}

// DecodeConfigurationEntry decodes and validates a Raft configuration entry.
func DecodeConfigurationEntry(data []byte) (model.Configuration, error) {
	if len(data) < configurationEntryHeaderSize {
		return model.Configuration{}, errors.New(
			"configuration entry is truncated",
		)
	}

	if len(data) > maxConfigurationPayload {
		return model.Configuration{}, errors.New(
			"configuration entry exceeds maximum payload size",
		)
	}

	reader := bytes.NewReader(data)

	var magic uint32
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return model.Configuration{}, fmt.Errorf(
			"decode configuration magic: %w",
			err,
		)
	}

	if magic != configurationEntryMagic {
		return model.Configuration{}, errors.New(
			"invalid configuration entry magic",
		)
	}

	version, err := reader.ReadByte()
	if err != nil {
		return model.Configuration{}, fmt.Errorf(
			"decode configuration version: %w",
			err,
		)
	}

	if version != configurationEntryVersion {
		return model.Configuration{}, fmt.Errorf(
			"unsupported configuration entry version: %d",
			version,
		)
	}

	entryType, err := reader.ReadByte()
	if err != nil {
		return model.Configuration{}, fmt.Errorf(
			"decode configuration entry type: %w",
			err,
		)
	}

	if entryType != configurationEntryType {
		return model.Configuration{}, fmt.Errorf(
			"unexpected configuration entry type: %d",
			entryType,
		)
	}

	var voterCount uint32
	if err := binary.Read(reader, binary.BigEndian, &voterCount); err != nil {
		return model.Configuration{}, fmt.Errorf(
			"decode voter count: %w",
			err,
		)
	}

	if voterCount == 0 {
		return model.Configuration{}, errors.New(
			"configuration must contain at least one voter",
		)
	}

	if voterCount > maxConfigurationVoters {
		return model.Configuration{}, fmt.Errorf(
			"configuration contains too many voters: %d",
			voterCount,
		)
	}

	voters := make([]model.NodeID, 0, voterCount)
	seen := make(map[model.NodeID]struct{}, voterCount)

	for i := uint32(0); i < voterCount; i++ {
		var idLength uint32

		if err := binary.Read(reader, binary.BigEndian, &idLength); err != nil {
			return model.Configuration{}, fmt.Errorf(
				"decode voter %d ID length: %w",
				i,
				err,
			)
		}

		if idLength == 0 {
			return model.Configuration{}, fmt.Errorf(
				"voter %d has empty ID",
				i,
			)
		}

		if idLength > maxConfigurationNodeID {
			return model.Configuration{}, fmt.Errorf(
				"voter %d ID is too long: %d",
				i,
				idLength,
			)
		}

		if uint64(idLength) > uint64(reader.Len()) {
			return model.Configuration{}, fmt.Errorf(
				"voter %d ID is truncated",
				i,
			)
		}

		idBytes := make([]byte, idLength)

		if _, err := reader.Read(idBytes); err != nil {
			return model.Configuration{}, fmt.Errorf(
				"decode voter %d ID: %w",
				i,
				err,
			)
		}

		voterID := model.NodeID(string(idBytes))

		if _, exists := seen[voterID]; exists {
			return model.Configuration{}, fmt.Errorf(
				"duplicate voter ID: %q",
				voterID,
			)
		}

		seen[voterID] = struct{}{}
		voters = append(voters, voterID)
	}

	if reader.Len() != 0 {
		return model.Configuration{}, errors.New(
			"configuration entry contains trailing data",
		)
	}

	configuration := model.Configuration{
		Voters: voters,
	}

	if err := validateConfiguration(configuration); err != nil {
		return model.Configuration{}, err
	}

	return configuration, nil
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
