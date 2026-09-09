package lock

import "github.com/sanchar127/raftiq/internal/model"

type Lock struct {
	Key          string
	OwnerID      string
	FencingToken uint64
	ExpiresAt    int64
	GrantIndex   model.LogIndex
}

type State struct {
	Locks     map[string]Lock
	NextToken uint64
}

func NewState() *State {
	return &State{
		Locks: make(map[string]Lock),
	}
}

func (s *State) Get(key string) (Lock, bool) {
	lock, ok := s.Locks[key]
	return lock, ok
}

func (s *State) Acquire(
	key string,
	ownerID string,
	expiresAt int64,
	grantIndex model.LogIndex,
) (Lock, bool) {
	current, exists := s.Locks[key]

	if exists {
		return current, false
	}

	s.NextToken++

	lock := Lock{
		Key:          key,
		OwnerID:      ownerID,
		FencingToken: s.NextToken,
		ExpiresAt:    expiresAt,
		GrantIndex:   grantIndex,
	}

	s.Locks[key] = lock

	return lock, true
}
func (s *State) Expire(
	key string,
	expectedToken uint64,
) (Lock, bool) {
	current, exists := s.Locks[key]
	if !exists {
		return Lock{}, false
	}

	if current.FencingToken != expectedToken {
		return current, false
	}

	delete(s.Locks, key)

	return current, true
}
