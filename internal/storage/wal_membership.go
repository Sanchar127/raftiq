package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/sanchar127/raftiq/internal/model"
)

const (
	stateMembershipMagic   uint32 = 0x52414654 // "RAFT"
	stateMembershipVersion byte   = 1
)

func encodeMembership(
	payload *bytes.Buffer,
	membership model.Membership,
) error {
	if err := binary.Write(
		payload,
		binary.BigEndian,
		stateMembershipMagic,
	); err != nil {
		return fmt.Errorf(
			"encode membership magic: %w",
			err,
		)
	}

	if err := payload.WriteByte(
		stateMembershipVersion,
	); err != nil {
		return fmt.Errorf(
			"encode membership version: %w",
			err,
		)
	}

	if err := encodeConfiguration(
		payload,
		membership.Current,
	); err != nil {
		return fmt.Errorf(
			"encode current configuration: %w",
			err,
		)
	}

	if membership.Joint == nil {
		if err := payload.WriteByte(0); err != nil {
			return fmt.Errorf(
				"encode joint configuration flag: %w",
				err,
			)
		}

		return nil
	}

	if err := payload.WriteByte(1); err != nil {
		return fmt.Errorf(
			"encode joint configuration flag: %w",
			err,
		)
	}

	if err := encodeConfiguration(
		payload,
		membership.Joint.Old,
	); err != nil {
		return fmt.Errorf(
			"encode joint old configuration: %w",
			err,
		)
	}

	if err := encodeConfiguration(
		payload,
		membership.Joint.New,
	); err != nil {
		return fmt.Errorf(
			"encode joint new configuration: %w",
			err,
		)
	}

	return nil
}

func encodeConfiguration(
	payload *bytes.Buffer,
	configuration model.Configuration,
) error {
	if uint64(len(configuration.Voters)) > uint64(^uint32(0)) {
		return fmt.Errorf(
			"too many configuration voters: %d",
			len(configuration.Voters),
		)
	}

	if err := binary.Write(
		payload,
		binary.BigEndian,
		uint32(len(configuration.Voters)),
	); err != nil {
		return fmt.Errorf(
			"encode voter count: %w",
			err,
		)
	}

	for i, voter := range configuration.Voters {
		data := []byte(voter)

		if uint64(len(data)) > uint64(^uint32(0)) {
			return fmt.Errorf(
				"voter %d ID too large: %d bytes",
				i,
				len(data),
			)
		}

		if err := binary.Write(
			payload,
			binary.BigEndian,
			uint32(len(data)),
		); err != nil {
			return fmt.Errorf(
				"encode voter %d length: %w",
				i,
				err,
			)
		}

		if _, err := payload.Write(data); err != nil {
			return fmt.Errorf(
				"encode voter %d: %w",
				i,
				err,
			)
		}
	}

	return nil
}

func decodeMembership(
	reader *bytes.Reader,
) (model.Membership, error) {
	var magic uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&magic,
	); err != nil {
		return model.Membership{}, fmt.Errorf(
			"decode membership magic: %w",
			err,
		)
	}

	if magic != stateMembershipMagic {
		return model.Membership{}, fmt.Errorf(
			"invalid membership magic: %08x",
			magic,
		)
	}

	version, err := reader.ReadByte()
	if err != nil {
		return model.Membership{}, fmt.Errorf(
			"decode membership version: %w",
			err,
		)
	}

	if version != stateMembershipVersion {
		return model.Membership{}, fmt.Errorf(
			"%w: membership version %d",
			ErrWALVersion,
			version,
		)
	}

	current, err := decodeConfiguration(reader)
	if err != nil {
		return model.Membership{}, fmt.Errorf(
			"decode current configuration: %w",
			err,
		)
	}

	jointFlag, err := reader.ReadByte()
	if err != nil {
		return model.Membership{}, fmt.Errorf(
			"decode joint configuration flag: %w",
			err,
		)
	}

	switch jointFlag {
	case 0:
		return model.Membership{
			Current: current,
		}, nil

	case 1:
		oldConfiguration, err := decodeConfiguration(reader)
		if err != nil {
			return model.Membership{}, fmt.Errorf(
				"decode joint old configuration: %w",
				err,
			)
		}

		newConfiguration, err := decodeConfiguration(reader)
		if err != nil {
			return model.Membership{}, fmt.Errorf(
				"decode joint new configuration: %w",
				err,
			)
		}

		return model.Membership{
			Current: current,
			Joint: &model.JointConfiguration{
				Old: oldConfiguration,
				New: newConfiguration,
			},
		}, nil

	default:
		return model.Membership{}, fmt.Errorf(
			"invalid joint configuration flag: %d",
			jointFlag,
		)
	}
}

func decodeConfiguration(
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

	var voters []model.NodeID

	if voterCount > 0 {
		voters = make(
			[]model.NodeID,
			0,
			voterCount,
		)
	}

	for i := uint32(0); i < voterCount; i++ {
		var length uint32

		if err := binary.Read(
			reader,
			binary.BigEndian,
			&length,
		); err != nil {
			return model.Configuration{}, fmt.Errorf(
				"decode voter %d length: %w",
				i,
				err,
			)
		}

		if uint64(length) > uint64(reader.Len()) {
			return model.Configuration{}, fmt.Errorf(
				"invalid voter %d length: %d, remaining payload: %d",
				i,
				length,
				reader.Len(),
			)
		}

		data := make([]byte, length)

		if _, err := io.ReadFull(reader, data); err != nil {
			return model.Configuration{}, fmt.Errorf(
				"decode voter %d: %w",
				i,
				err,
			)
		}

		voters = append(
			voters,
			model.NodeID(data),
		)
	}

	return model.Configuration{
		Voters: voters,
	}, nil
}
