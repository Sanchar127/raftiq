package transport

import (
	"context"
	"errors"
	"testing"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/stretchr/testify/require"
)

type fakeKVStore struct {
	getValue  []byte
	getFound  bool
	getErr    error
	putErr    error
	deleteErr error

	getKey    string
	putKey    string
	putValue  []byte
	deleteKey string
}

func (f *fakeKVStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	f.getKey = key
	return f.getValue, f.getFound, f.getErr
}

func (f *fakeKVStore) Put(_ context.Context, key string, value []byte) error {
	f.putKey = key
	f.putValue = append([]byte(nil), value...)
	return f.putErr
}

func (f *fakeKVStore) Delete(_ context.Context, key string) error {
	f.deleteKey = key
	return f.deleteErr
}

func TestNewKVService(t *testing.T) {
	t.Parallel()

	service, err := NewKVService(&fakeKVStore{})

	require.NoError(t, err)
	require.NotNil(t, service)
}

func TestNewKVServiceRejectsNilStore(t *testing.T) {
	t.Parallel()

	service, err := NewKVService(nil)

	require.Error(t, err)
	require.Nil(t, service)
	require.ErrorContains(t, err, "kv store is required")
}

func TestKVServiceGet(t *testing.T) {
	t.Parallel()

	store := &fakeKVStore{
		getValue: []byte("value"),
		getFound: true,
	}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Get(
		context.Background(),
		&raftiqv1.GetRequest{
			Key: "hello",
		},
	)

	require.NoError(t, err)
	require.True(t, response.GetFound())
	require.Equal(t, []byte("value"), response.GetValue())
	require.Equal(t, "hello", store.getKey)
}

func TestKVServiceGetNotFound(t *testing.T) {
	t.Parallel()

	store := &fakeKVStore{}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Get(
		context.Background(),
		&raftiqv1.GetRequest{
			Key: "missing",
		},
	)

	require.NoError(t, err)
	require.False(t, response.GetFound())
	require.Empty(t, response.GetValue())
}

func TestKVServiceGetPropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("storage failure")

	store := &fakeKVStore{
		getErr: expectedErr,
	}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Get(
		context.Background(),
		&raftiqv1.GetRequest{
			Key: "hello",
		},
	)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, response)
}

func TestKVServiceGetRejectsNilRequest(t *testing.T) {
	t.Parallel()

	service, err := NewKVService(&fakeKVStore{})
	require.NoError(t, err)

	response, err := service.Get(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "get request is required")
}

func TestKVServicePut(t *testing.T) {
	t.Parallel()

	store := &fakeKVStore{}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Put(
		context.Background(),
		&raftiqv1.PutRequest{
			Key:   "hello",
			Value: []byte("world"),
		},
	)

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, "hello", store.putKey)
	require.Equal(t, []byte("world"), store.putValue)
}

func TestKVServicePutPropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("write failure")

	store := &fakeKVStore{
		putErr: expectedErr,
	}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Put(
		context.Background(),
		&raftiqv1.PutRequest{
			Key:   "hello",
			Value: []byte("world"),
		},
	)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, response)
}

func TestKVServicePutRejectsNilRequest(t *testing.T) {
	t.Parallel()

	service, err := NewKVService(&fakeKVStore{})
	require.NoError(t, err)

	response, err := service.Put(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "put request is required")
}

func TestKVServiceDelete(t *testing.T) {
	t.Parallel()

	store := &fakeKVStore{}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Delete(
		context.Background(),
		&raftiqv1.DeleteRequest{
			Key: "hello",
		},
	)

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, "hello", store.deleteKey)
}

func TestKVServiceDeletePropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("delete failure")

	store := &fakeKVStore{
		deleteErr: expectedErr,
	}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Delete(
		context.Background(),
		&raftiqv1.DeleteRequest{
			Key: "hello",
		},
	)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, response)
}

func TestKVServiceDeleteRejectsNilRequest(t *testing.T) {
	t.Parallel()

	service, err := NewKVService(&fakeKVStore{})
	require.NoError(t, err)

	response, err := service.Delete(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "delete request is required")
}

func TestKVServiceGetCopiesValue(t *testing.T) {
	t.Parallel()

	store := &fakeKVStore{
		getValue: []byte("original"),
		getFound: true,
	}

	service, err := NewKVService(store)
	require.NoError(t, err)

	response, err := service.Get(
		context.Background(),
		&raftiqv1.GetRequest{
			Key: "hello",
		},
	)
	require.NoError(t, err)

	store.getValue[0] = 'X'

	require.Equal(t, []byte("original"), response.GetValue())
}

var _ kvRPC = (*fakeKVStore)(nil)
