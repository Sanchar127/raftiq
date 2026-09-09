package lock

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestStateFencingTokensRemainMonotonicAcrossExpiration(t *testing.T) {
	state := NewState()

	first, acquired := state.Acquire(
		"job:1",
		"worker-A",
		100,
		model.LogIndex(1),
	)
	if !acquired {
		t.Fatal("expected first acquisition")
	}

	if first.FencingToken != 1 {
		t.Fatalf(
			"first token: got %d, want 1",
			first.FencingToken,
		)
	}

	_, expired := state.Expire(
		"job:1",
		first.FencingToken,
	)
	if !expired {
		t.Fatal("expected first lock to expire")
	}

	if state.NextToken != 1 {
		t.Fatalf(
			"token counter changed after expiration: got %d, want 1",
			state.NextToken,
		)
	}

	second, acquired := state.Acquire(
		"job:1",
		"worker-B",
		200,
		model.LogIndex(2),
	)
	if !acquired {
		t.Fatal("expected second acquisition")
	}

	if second.FencingToken != 2 {
		t.Fatalf(
			"second token: got %d, want 2",
			second.FencingToken,
		)
	}

	_, expired = state.Expire(
		"job:1",
		second.FencingToken,
	)
	if !expired {
		t.Fatal("expected second lock to expire")
	}

	if state.NextToken != 2 {
		t.Fatalf(
			"token counter changed after second expiration: got %d, want 2",
			state.NextToken,
		)
	}

	third, acquired := state.Acquire(
		"job:1",
		"worker-C",
		300,
		model.LogIndex(3),
	)
	if !acquired {
		t.Fatal("expected third acquisition")
	}

	if third.FencingToken != 3 {
		t.Fatalf(
			"third token: got %d, want 3",
			third.FencingToken,
		)
	}
}
func TestStateFencingTokensAreGlobalAcrossKeys(t *testing.T) {
	state := NewState()

	first, acquired := state.Acquire(
		"job:1",
		"worker-A",
		100,
		model.LogIndex(1),
	)
	if !acquired {
		t.Fatal("expected first acquisition")
	}

	second, acquired := state.Acquire(
		"job:2",
		"worker-B",
		100,
		model.LogIndex(2),
	)
	if !acquired {
		t.Fatal("expected second acquisition")
	}

	if first.FencingToken != 1 {
		t.Fatalf(
			"job:1 token: got %d, want 1",
			first.FencingToken,
		)
	}

	if second.FencingToken != 2 {
		t.Fatalf(
			"job:2 token: got %d, want 2",
			second.FencingToken,
		)
	}

	if state.NextToken != 2 {
		t.Fatalf(
			"next token: got %d, want 2",
			state.NextToken,
		)
	}
}

func TestStateStaleExpirationCannotRemoveNewOwner(t *testing.T) {
	state := NewState()

	first, acquired := state.Acquire(
		"job:1",
		"worker-A",
		100,
		model.LogIndex(1),
	)
	if !acquired {
		t.Fatal("expected first acquisition")
	}

	_, expired := state.Expire(
		"job:1",
		first.FencingToken,
	)
	if !expired {
		t.Fatal("expected first expiration")
	}

	second, acquired := state.Acquire(
		"job:1",
		"worker-B",
		200,
		model.LogIndex(2),
	)
	if !acquired {
		t.Fatal("expected second acquisition")
	}

	_, expired = state.Expire(
		"job:1",
		first.FencingToken,
	)
	if expired {
		t.Fatal("stale expiration removed the new owner")
	}

	current, ok := state.Get("job:1")
	if !ok {
		t.Fatal("new lock disappeared")
	}

	if current.OwnerID != "worker-B" {
		t.Fatalf(
			"owner: got %q, want worker-B",
			current.OwnerID,
		)
	}

	if current.FencingToken != second.FencingToken {
		t.Fatalf(
			"token: got %d, want %d",
			current.FencingToken,
			second.FencingToken,
		)
	}
}
