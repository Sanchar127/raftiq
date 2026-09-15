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
