package storage

import "time"

type StorageMetrics interface {
	IncOperation(operation string)
	IncOperationError(operation string)
	ObserveOperationDuration(operation string, duration time.Duration)

	IncSync()
	IncSyncError()
	ObserveSyncDuration(duration time.Duration)
}

type NoopStorageMetrics struct{}

func (NoopStorageMetrics) IncOperation(string) {}
func (NoopStorageMetrics) IncOperationError(string) {}
func (NoopStorageMetrics) ObserveOperationDuration(string, time.Duration) {}

func (NoopStorageMetrics) IncSync() {}
func (NoopStorageMetrics) IncSyncError() {}
func (NoopStorageMetrics) ObserveSyncDuration(time.Duration) {}