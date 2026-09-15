package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

func EncodeEnterJointConfigurationEntry(
	oldConfiguration model.Configuration,
	newConfiguration model.Configuration,
) ([]byte, error) {
	if err := validateConfiguration(oldConfiguration); err != nil {
		return nil, fmt.Errorf("validate old configuration: %w", err)
	}

	if err := validateConfiguration(newConfiguration); err != nil {
		return nil, fmt.Errorf("validate new configuration: %w", err)
	}

	var buf bytes.Buffer

	if err := binary.Write(
		&buf,
		binary.BigEndian,
		configurationEntryMagic,
	); err != nil {
		return nil, fmt.Errorf("encode configuration magic: %w", err)
	}

	if err := buf.WriteByte(configurationEntryVersion); err != nil {
		return nil, fmt.Errorf("encode configuration version: %w", err)
	}

	if err := buf.WriteByte(configurationEntryTypeEnterJoint); err != nil {
		return nil, fmt.Errorf("encode configuration entry type: %w", err)
	}

	if err := encodeConfigurationPayload(&buf, oldConfiguration); err != nil {
		return nil, fmt.Errorf("encode old configuration: %w", err)
	}

	if err := encodeConfigurationPayload(&buf, newConfiguration); err != nil {
		return nil, fmt.Errorf("encode new configuration: %w", err)
	}

	if buf.Len() > maxConfigurationPayload {
		return nil, errors.New(
			"joint configuration entry exceeds maximum payload size",
		)
	}

	return buf.Bytes(), nil
}

func EncodeLeaveJointConfigurationEntry(
	newConfiguration model.Configuration,
) ([]byte, error) {
	if err := validateConfiguration(newConfiguration); err != nil {
		return nil, fmt.Errorf(
			"validate new configuration: %w",
			err,
		)
	}

	var buf bytes.Buffer

	if err := binary.Write(
		&buf,
		binary.BigEndian,
		configurationEntryMagic,
	); err != nil {
		return nil, fmt.Errorf("encode configuration magic: %w", err)
	}

	if err := buf.WriteByte(configurationEntryVersion); err != nil {
		return nil, fmt.Errorf("encode configuration version: %w", err)
	}

	if err := buf.WriteByte(configurationEntryTypeLeaveJoint); err != nil {
		return nil, fmt.Errorf("encode configuration entry type: %w", err)
	}

	if err := encodeConfigurationPayload(&buf, newConfiguration); err != nil {
		return nil, fmt.Errorf("encode new configuration: %w", err)
	}

	if buf.Len() > maxConfigurationPayload {
		return nil, errors.New(
			"leave-joint configuration entry exceeds maximum payload size",
		)
	}

	return buf.Bytes(), nil
}

func DecodeEnterJointConfigurationEntry(
	data []byte,
) (model.Configuration, model.Configuration, error) {
	if len(data) > maxConfigurationPayload {
		return model.Configuration{}, model.Configuration{}, errors.New(
			"joint configuration entry exceeds maximum payload size",
		)
	}

	reader := bytes.NewReader(data)

	if err := validateConfigurationEntryEnvelope(
		reader,
		configurationEntryTypeEnterJoint,
	); err != nil {
		return model.Configuration{}, model.Configuration{}, err
	}

	oldConfiguration, err := decodeConfigurationPayload(reader)
	if err != nil {
		return model.Configuration{}, model.Configuration{}, fmt.Errorf(
			"decode old configuration: %w",
			err,
		)
	}

	newConfiguration, err := decodeConfigurationPayload(reader)
	if err != nil {
		return model.Configuration{}, model.Configuration{}, fmt.Errorf(
			"decode new configuration: %w",
			err,
		)
	}

	if reader.Len() != 0 {
		return model.Configuration{}, model.Configuration{}, errors.New(
			"joint configuration entry contains trailing data",
		)
	}

	return oldConfiguration, newConfiguration, nil
}

func DecodeLeaveJointConfigurationEntry(
	data []byte,
) (model.Configuration, error) {
	if len(data) > maxConfigurationPayload {
		return model.Configuration{}, errors.New(
			"leave-joint configuration entry exceeds maximum payload size",
		)
	}

	reader := bytes.NewReader(data)

	if err := validateConfigurationEntryEnvelope(
		reader,
		configurationEntryTypeLeaveJoint,
	); err != nil {
		return model.Configuration{}, err
	}

	configuration, err := decodeConfigurationPayload(reader)
	if err != nil {
		return model.Configuration{}, fmt.Errorf(
			"decode new configuration: %w",
			err,
		)
	}

	if reader.Len() != 0 {
		return model.Configuration{}, errors.New(
			"leave-joint configuration entry contains trailing data",
		)
	}

	return configuration, nil
}
