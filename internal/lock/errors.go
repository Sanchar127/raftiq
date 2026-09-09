package lock

import "errors"

var (
	ErrLockBusy          = errors.New("lock is already held")
	ErrLockNotFound      = errors.New("lock not found")
	ErrNotOwner          = errors.New("lock owner mismatch")
	ErrInvalidOwner      = errors.New("invalid lock owner")
	ErrInvalidKey        = errors.New("invalid lock key")
	ErrStaleFencingToken = errors.New("stale fencing token")
)
