package kv

import "errors"

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrInvalidJob        = errors.New("invalid job")
	ErrJobNotClaimable   = errors.New("job is not claimable")
	ErrJobAlreadyClaimed = errors.New("job is already claimed")
	ErrInvalidJobState   = errors.New("invalid job state transition")
	ErrJobOwnershipLost  = errors.New("job ownership lost")
)