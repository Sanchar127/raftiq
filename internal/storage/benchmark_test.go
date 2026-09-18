package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func BenchmarkWALAppendEntries(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "benchmark.wal")

	store, err := OpenWAL(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	entry := model.LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("raftiq-benchmark-value"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry.Index = model.LogIndex(i + 1)

		if err := store.AppendEntries([]model.LogEntry{entry}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALSync(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "benchmark-sync.wal")

	store, err := OpenWAL(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	entry := model.LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("raftiq-benchmark-value"),
	}

	if err := store.AppendEntries([]model.LogEntry{entry}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := store.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALRecovery(b *testing.B) {
	for _, entryCount := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", entryCount), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "benchmark-recovery.wal")

			store, err := OpenWAL(path)
			if err != nil {
				b.Fatal(err)
			}

			entries := make([]model.LogEntry, entryCount)
			for i := range entries {
				entries[i] = model.LogEntry{
					Index: model.LogIndex(i + 1),
					Term:  1,
					Data:  []byte("raftiq-benchmark-value"),
				}
			}

			if err := store.AppendEntries(entries); err != nil {
				store.Close()
				b.Fatal(err)
			}

			if err := store.Sync(); err != nil {
				store.Close()
				b.Fatal(err)
			}

			if err := store.Close(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				recovered, err := OpenWAL(path)
				if err != nil {
					b.Fatal(err)
				}

				if err := recovered.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
