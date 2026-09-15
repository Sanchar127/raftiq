package storage

import "errors"

var (
	ErrInvalidLog    = errors.New("invalid log")
	ErrWALCorrupt    = errors.New("wal corrupt")
	ErrWALVersion    = errors.New("unsupported wal version")
	ErrWALDiskFull   = errors.New("wal disk full")
	ErrClosedStorage = errors.New("error in closed storage")
)
