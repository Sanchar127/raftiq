package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

func (s *WALStorage) SaveState(
	state model.PersistentState,
) (err error) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationSaveState)

		if err != nil {
			metrics.IncOperationError(StorageOperationSaveState)
		}

		metrics.ObserveOperationDuration(
			StorageOperationSaveState,
			time.Since(start),
		)
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	metrics = s.metrics
	if metrics == nil {
		metrics = NoopStorageMetrics{}
	}

	if err = s.ensureOpen(); err != nil {
		return err
	}

	record, err := encodeStateRecord(state)
	if err != nil {
		return fmt.Errorf("encode state record: %w", err)
	}

	if err = writeFull(s.file, record); err != nil {
		return fmt.Errorf("write state record: %w", err)
	}

	s.state = state

	return nil
}

func (s *WALStorage) LoadState() (
	state model.PersistentState,
	err error,
) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationLoadState)

		if err != nil {
			metrics.IncOperationError(StorageOperationLoadState)
		}

		metrics.ObserveOperationDuration(
			StorageOperationLoadState,
			time.Since(start),
		)
	}()

	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics = s.metrics
	if metrics == nil {
		metrics = NoopStorageMetrics{}
	}

	if err = s.ensureOpenRead(); err != nil {
		return model.PersistentState{}, err
	}

	return s.state, nil
}

func encodeStateRecord(
	state model.PersistentState,
) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint64(state.CurrentTerm),
	); err != nil {
		return nil, fmt.Errorf(
			"encode current term: %w",
			err,
		)
	}

	votedFor := []byte(state.VotedFor)

	if uint64(len(votedFor)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf(
			"voted-for value too large: %d bytes",
			len(votedFor),
		)
	}

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint32(len(votedFor)),
	); err != nil {
		return nil, fmt.Errorf(
			"encode voted-for length: %w",
			err,
		)
	}

	if _, err := payload.Write(votedFor); err != nil {
		return nil, fmt.Errorf(
			"encode voted-for: %w",
			err,
		)
	}

	if err := encodeMembership(
		&payload,
		state.Membership,
	); err != nil {
		return nil, fmt.Errorf(
			"encode membership: %w",
			err,
		)
	}

	return encodeRecord(
		recordState,
		payload.Bytes(),
	)
}

func decodeStatePayload(
	payload []byte,
) (model.PersistentState, error) {
	reader := bytes.NewReader(payload)

	var term uint64

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&term,
	); err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode current term: %w",
			err,
		)
	}

	var votedForLength uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&votedForLength,
	); err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode voted-for length: %w",
			err,
		)
	}

	if uint64(votedForLength) > uint64(reader.Len()) {
		return model.PersistentState{}, fmt.Errorf(
			"invalid voted-for length: %d, remaining payload: %d",
			votedForLength,
			reader.Len(),
		)
	}

	votedFor := make(
		[]byte,
		votedForLength,
	)

	if _, err := io.ReadFull(
		reader,
		votedFor,
	); err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode voted-for: %w",
			err,
		)
	}

	state := model.PersistentState{
		CurrentTerm: model.Term(term),
		VotedFor:    model.NodeID(votedFor),
	}

	// No extension means this is an old WAL state record.
	if reader.Len() == 0 {
		return state, nil
	}

	membership, err := decodeMembership(reader)
	if err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode membership: %w",
			err,
		)
	}

	if reader.Len() != 0 {
		return model.PersistentState{}, fmt.Errorf(
			"unexpected trailing state data: %d bytes",
			reader.Len(),
		)
	}

	state.Membership = membership

	return state, nil
}
