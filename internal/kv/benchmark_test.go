package kv

import "testing"

func BenchmarkStorePut(b *testing.B) {
	store := NewStore()
	value := []byte("raftiq-benchmark-value")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Put("benchmark-key", value)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	store := NewStore()
	store.Put("benchmark-key", []byte("raftiq-benchmark-value"))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = store.Get("benchmark-key")
	}
}

func BenchmarkEncodeCommand(b *testing.B) {
	command := Command{
		Type:  CommandPut,
		Key:   "benchmark-key",
		Value: []byte("raftiq-benchmark-value"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = EncodeCommand(command)
	}
}
