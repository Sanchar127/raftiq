package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	recordState          byte   = 1
	recordEntries        byte   = 2
	recordSnapshot       byte   = 3
	recordReplaceSuffix  byte   = 4
	maxRecordPayloadSize uint32 = 16 << 20

	recordHeaderSize = 1 + 4
	recordFooterSize = 4
)

func encodeRecord(
	recordType byte,
	payload []byte,
) ([]byte, error) {
	if uint64(len(payload)) > uint64(maxRecordPayloadSize) {
		return nil, fmt.Errorf(
			"record payload too large: %d bytes, maximum %d",
			len(payload),
			maxRecordPayloadSize,
		)
	}

	var record bytes.Buffer

	if err := record.WriteByte(recordType); err != nil {
		return nil, fmt.Errorf(
			"encode record type: %w",
			err,
		)
	}

	if err := binary.Write(
		&record,
		binary.BigEndian,
		uint32(len(payload)),
	); err != nil {
		return nil, fmt.Errorf(
			"encode record length: %w",
			err,
		)
	}

	if _, err := record.Write(payload); err != nil {
		return nil, fmt.Errorf(
			"encode record payload: %w",
			err,
		)
	}

	checksum := crc32.ChecksumIEEE(record.Bytes())

	if err := binary.Write(
		&record,
		binary.BigEndian,
		checksum,
	); err != nil {
		return nil, fmt.Errorf(
			"encode record checksum: %w",
			err,
		)
	}

	return record.Bytes(), nil
}

func decodeRecord(
	reader io.Reader,
) (byte, []byte, error) {
	var recordType byte

	// EOF before reading any record bytes means the WAL ended cleanly.
	if err := binary.Read(
		reader,
		binary.BigEndian,
		&recordType,
	); err != nil {
		return 0, nil, err
	}

	var payloadLength uint32

	// Once the record type has been read, EOF means the record is
	// incomplete rather than a clean end of the WAL.
	if err := binary.Read(
		reader,
		binary.BigEndian,
		&payloadLength,
	); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}

		return 0, nil, err
	}

	if payloadLength > maxRecordPayloadSize {
		return 0, nil, fmt.Errorf(
			"WAL record payload too large: %d bytes, maximum %d",
			payloadLength,
			maxRecordPayloadSize,
		)
	}

	payload := make([]byte, payloadLength)

	if _, err := io.ReadFull(reader, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}

		return 0, nil, err
	}

	var storedChecksum uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&storedChecksum,
	); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}

		return 0, nil, err
	}

	var headerAndPayload bytes.Buffer

	if err := headerAndPayload.WriteByte(recordType); err != nil {
		return 0, nil, err
	}

	if err := binary.Write(
		&headerAndPayload,
		binary.BigEndian,
		payloadLength,
	); err != nil {
		return 0, nil, err
	}

	if _, err := headerAndPayload.Write(payload); err != nil {
		return 0, nil, err
	}

	expectedChecksum := crc32.ChecksumIEEE(headerAndPayload.Bytes())

	if storedChecksum != expectedChecksum {
		return 0, nil, fmt.Errorf(
			"WAL checksum mismatch: got %08x, want %08x",
			storedChecksum,
			expectedChecksum,
		)
	}

	return recordType, payload, nil
}

func writeFull(
	file *os.File,
	data []byte,
) error {
	if file == nil {
		return ErrClosedStorage
	}

	for len(data) > 0 {
		n, err := file.Write(data)

		if n > 0 {
			data = data[n:]
		}

		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}
	}

	return nil
}
