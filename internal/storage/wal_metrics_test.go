package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sanchar127/raftiq/internal/model"
)

type fakeStorageMetrics struct {
	mu sync.Mutex

	operations         map[string]int
	operationErrors    map[string]int
	operationDurations map[string]int

	syncs         int
	syncErrors    int
	syncDurations int
}

func newFakeStorageMetrics() *fakeStorageMetrics {
	return &fakeStorageMetrics{
		operations:         make(map[string]int),
		operationErrors:    make(map[string]int),
		operationDurations: make(map[string]int),
	}
}

func (m *fakeStorageMetrics) IncOperation(operation string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.operations[operation]++
}

func (m *fakeStorageMetrics) IncOperationError(operation string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.operationErrors[operation]++
}

func (m *fakeStorageMetrics) ObserveOperationDuration(
	operation string,
	_ time.Duration,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.operationDurations[operation]++
}

func (m *fakeStorageMetrics) IncSync() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.syncs++
}

func (m *fakeStorageMetrics) IncSyncError() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.syncErrors++
}

func (m *fakeStorageMetrics) ObserveSyncDuration(_ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.syncDurations++
}

func (m *fakeStorageMetrics) operationCount(operation string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.operations[operation]
}

func (m *fakeStorageMetrics) operationErrorCount(operation string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.operationErrors[operation]
}

func (m *fakeStorageMetrics) operationDurationCount(operation string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.operationDurations[operation]
}

func (m *fakeStorageMetrics) syncCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.syncs
}

func (m *fakeStorageMetrics) syncErrorCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.syncErrors
}

func (m *fakeStorageMetrics) syncDurationCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.syncDurations
}

func TestWALStorageMetricsSuccessfulOperations(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/raftiq.wal"

	storage, err := OpenWAL(path)
	require.NoError(t, err)
	defer storage.Close()

	metrics := newFakeStorageMetrics()
	storage.SetMetrics(metrics)

	state := model.PersistentState{
		CurrentTerm: 1,
		VotedFor:    "node-1",
	}

	entry := model.LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("command"),
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot"),
	}

	require.NoError(t, storage.SaveState(state))
	_, err = storage.LoadState()
	require.NoError(t, err)

	require.NoError(t, storage.AppendEntries([]model.LogEntry{entry}))
	_, err = storage.LoadEntries()
	require.NoError(t, err)

	require.NoError(t, storage.ReplaceSuffix(
		1,
		[]model.LogEntry{entry},
	))

	require.NoError(t, storage.SaveSnapshot(snapshot))
	_, err = storage.LoadSnapshot()
	require.NoError(t, err)

	require.NoError(t, storage.Sync())

	operations := []string{
		StorageOperationSaveState,
		StorageOperationLoadState,
		StorageOperationAppendEntries,
		StorageOperationLoadEntries,
		StorageOperationReplaceSuffix,
		StorageOperationSaveSnapshot,
		StorageOperationLoadSnapshot,
	}

	for _, operation := range operations {
		require.Equal(t, 1, metrics.operationCount(operation), operation)
		require.Equal(t, 0, metrics.operationErrorCount(operation), operation)
		require.Equal(t, 1, metrics.operationDurationCount(operation), operation)
	}

	require.Equal(t, 1, metrics.syncCount())
	require.Equal(t, 0, metrics.syncErrorCount())
	require.Equal(t, 1, metrics.syncDurationCount())
}

func TestWALStorageMetricsFailedOperation(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/raftiq.wal"

	storage, err := OpenWAL(path)
	require.NoError(t, err)

	metrics := newFakeStorageMetrics()
	storage.SetMetrics(metrics)

	require.NoError(t, storage.Close())

	err = storage.SaveState(model.PersistentState{
		CurrentTerm: 1,
	})

	require.ErrorIs(t, err, ErrClosedStorage)

	require.Equal(
		t,
		1,
		metrics.operationCount(StorageOperationSaveState),
	)

	require.Equal(
		t,
		1,
		metrics.operationErrorCount(StorageOperationSaveState),
	)

	require.Equal(
		t,
		1,
		metrics.operationDurationCount(StorageOperationSaveState),
	)
}

func TestWALStorageMetricsFailedSync(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/raftiq.wal"

	storage, err := OpenWAL(path)
	require.NoError(t, err)

	metrics := newFakeStorageMetrics()
	storage.SetMetrics(metrics)

	require.NoError(t, storage.Close())

	err = storage.Sync()

	require.ErrorIs(t, err, ErrClosedStorage)

	require.Equal(t, 1, metrics.syncCount())
	require.Equal(t, 1, metrics.syncErrorCount())
	require.Equal(t, 1, metrics.syncDurationCount())
}

func TestWALStorageSetMetricsNilRestoresNoop(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/raftiq.wal"

	storage, err := OpenWAL(path)
	require.NoError(t, err)
	defer storage.Close()

	metrics := newFakeStorageMetrics()

	storage.SetMetrics(metrics)
	storage.SetMetrics(nil)

	require.NoError(t, storage.SaveState(model.PersistentState{
		CurrentTerm: 1,
	}))

	require.Equal(
		t,
		0,
		metrics.operationCount(StorageOperationSaveState),
	)
}
