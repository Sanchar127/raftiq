package storage

import (
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

func validateEntries(
	entries []model.LogEntry,
) error {
	for i, entry := range entries {
		if entry.Index == 0 {
			return fmt.Errorf(
				"%w: entry %d has zero index",
				ErrInvalidLog,
				i,
			)
		}

		if i > 0 {
			previous := entries[i-1]

			if entry.Index != previous.Index+1 {
				return fmt.Errorf(
					"%w: entry indexes are not contiguous: %d followed by %d",
					ErrInvalidLog,
					previous.Index,
					entry.Index,
				)
			}
		}
	}

	return nil
}

func validateLog(
	entries []model.LogEntry,
) error {
	if len(entries) == 0 {
		return nil
	}

	return validateEntries(entries)
}

func validateAppend(
	existing []model.LogEntry,
	additions []model.LogEntry,
) error {
	if len(existing) == 0 {
		return nil
	}

	expected := existing[len(existing)-1].Index + 1

	if additions[0].Index != expected {
		return fmt.Errorf(
			"%w: append starts at index %d, want %d",
			ErrInvalidLog,
			additions[0].Index,
			expected,
		)
	}

	return nil
}

func validateReplaceSuffix(
	snapshot *model.Snapshot,
	from model.LogIndex,
	entries []model.LogEntry,
) error {
	if from == 0 {
		return fmt.Errorf(
			"%w: suffix replacement index must be greater than zero",
			ErrInvalidLog,
		)
	}

	if snapshot != nil && from <= snapshot.LastIncludedIndex {
		return fmt.Errorf(
			"%w: suffix replacement index %d is at or before snapshot index %d",
			ErrInvalidLog,
			from,
			snapshot.LastIncludedIndex,
		)
	}

	if err := validateEntries(entries); err != nil {
		return fmt.Errorf(
			"validate replacement entries: %w",
			err,
		)
	}

	if len(entries) > 0 && entries[0].Index != from {
		return fmt.Errorf(
			"%w: replacement starts at index %d, want %d",
			ErrInvalidLog,
			entries[0].Index,
			from,
		)
	}

	return nil
}

func validateRecoveredAppend(
	existing []model.LogEntry,
	additions []model.LogEntry,
) error {
	if len(additions) == 0 {
		return nil
	}

	return validateAppend(
		existing,
		additions,
	)
}

func validateSnapshot(
	snapshot model.Snapshot,
) error {
	if snapshot.LastIncludedIndex == 0 {
		return fmt.Errorf(
			"%w: snapshot index must be greater than zero",
			ErrInvalidLog,
		)
	}

	return nil
}

func validateRecoveredSnapshot(
	current *model.Snapshot,
	snapshot model.Snapshot,
) error {
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}

	if current == nil {
		return nil
	}

	if snapshot.LastIncludedIndex < current.LastIncludedIndex {
		return fmt.Errorf(
			"%w: snapshot index moved backwards from %d to %d",
			ErrInvalidLog,
			current.LastIncludedIndex,
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedIndex == current.LastIncludedIndex &&
		snapshot.LastIncludedTerm != current.LastIncludedTerm {
		return fmt.Errorf(
			"%w: snapshot term changed at index %d from %d to %d",
			ErrInvalidLog,
			snapshot.LastIncludedIndex,
			current.LastIncludedTerm,
			snapshot.LastIncludedTerm,
		)
	}

	return nil
}
