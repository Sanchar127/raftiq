package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

func encodeConfigurationPayload(
	buf *bytes.Buffer,
	configuration model.Configuration,
) error {
	if err := binary.Write(
		buf,
		binary.BigEndian,
		uint32(len(configuration.Voters)),
	); err != nil {
		return fmt.Errorf("encode voter count: %w", err)
	}

	for _, voterID := range configuration.Voters {
		id := []byte(voterID)

		if err := binary.Write(
			buf,
			binary.BigEndian,
			uint32(len(id)),
		); err != nil {
			return fmt.Errorf("encode voter ID length: %w", err)
		}

		if _, err := buf.Write(id); err != nil {
			return fmt.Errorf("encode voter ID: %w", err)
		}
	}

	return nil
}

func decodeConfigurationPayload(
	reader *bytes.Reader,
) (model.Configuration, error) {
	var voterCount uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&voterCount,
	); err != nil {
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

		if err := binary.Read(
			reader,
			binary.BigEndian,
			&idLength,
		); err != nil {
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

	configuration := model.Configuration{
		Voters: voters,
	}

	if err := validateConfiguration(configuration); err != nil {
		return model.Configuration{}, err
	}

	return configuration, nil
}

func validateConfigurationEntryEnvelope(
	reader *bytes.Reader,
	expectedType byte,
) error {
	var magic uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&magic,
	); err != nil {
		return fmt.Errorf(
			"decode configuration magic: %w",
			err,
		)
	}

	if magic != configurationEntryMagic {
		return errors.New(
			"invalid configuration entry magic",
		)
	}

	version, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf(
			"decode configuration version: %w",
			err,
		)
	}

	if version != configurationEntryVersion {
		return fmt.Errorf(
			"unsupported configuration entry version: %d",
			version,
		)
	}

	entryType, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf(
			"decode configuration entry type: %w",
			err,
		)
	}

	if entryType != expectedType {
		return fmt.Errorf(
			"unexpected configuration entry type: %d",
			entryType,
		)
	}

	return nil
}
