package storage

import "github.com/sanchar127/raftiq/internal/model"

func replaceSuffixCopy(
	existing []model.LogEntry,
	from model.LogIndex,
	replacement []model.LogEntry,
) []model.LogEntry {
	cut := len(existing)

	for i, entry := range existing {
		if entry.Index >= from {
			cut = i
			break
		}
	}

	result := make(
		[]model.LogEntry,
		0,
		cut+len(replacement),
	)

	result = append(
		result,
		existing[:cut]...,
	)

	result = appendEntriesCopy(
		result,
		replacement,
	)

	return result
}

func appendEntriesCopy(
	dst []model.LogEntry,
	src []model.LogEntry,
) []model.LogEntry {
	for _, entry := range src {
		copied := entry
		copied.Data = cloneBytes(entry.Data)
		dst = append(dst, copied)
	}

	return dst
}

func cloneEntries(
	entries []model.LogEntry,
) []model.LogEntry {
	if len(entries) == 0 {
		return make([]model.LogEntry, 0)
	}

	result := make(
		[]model.LogEntry,
		len(entries),
	)

	for i, entry := range entries {
		result[i] = entry
		result[i].Data = cloneBytes(entry.Data)
	}

	return result
}

func cloneBytes(
	data []byte,
) []byte {
	if data == nil {
		return nil
	}

	return append(
		[]byte(nil),
		data...,
	)
}
