#!/usr/bin/env python3
"""
Split grpc_raft_cluster_test.go into scenario files.

- Parses the original file for top-level funcs and types.
- Routes each to the target file.
- Preserves bodies and comments exactly.
- Removes the original file.
- Assumes all files use the same import set (safe for this package).
"""
import os
import re
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)

ORIGINAL = "grpc_raft_cluster_test.go"
BACKUP = ORIGINAL + ".bak"

if not os.path.isfile(ORIGINAL):
    print(f"error: {ORIGINAL} not found in {os.getcwd()}")
    sys.exit(1)

shutil.copy(ORIGINAL, BACKUP)

with open(ORIGINAL, "r") as f:
    src = f.read()

# -----------------------------------------------------------------------------
# Parse top-level declarations. A top-level decl starts at column 0 with
# "func ", "type ", or "var " and ends at the next line beginning with
# "func ", "type ", "var ", or EOF.
# -----------------------------------------------------------------------------
DECL_RE = re.compile(r'^(func |type |var )', re.MULTILINE)

positions = [m.start() for m in DECL_RE.finditer(src)]

if not positions:
    print("error: no top-level declarations found")
    sys.exit(1)

# Add virtual end
positions.append(len(src))

decls = []
for i in range(len(positions) - 1):
    block = src[positions[i]:positions[i+1]].rstrip() + "\n\n"
    decls.append(block)

def decl_name(block):
    """Return the identifier declared by this block."""
    # func (r *T) Name(...) -> "T.Name"
    m = re.match(r'func\s+\(\s*\w+\s+\*?(\w+)\s*\)\s+(\w+)', block)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    # func Name(...)
    m = re.match(r'func\s+(\w+)', block)
    if m:
        return m.group(1)
    # type Name
    m = re.match(r'type\s+(\w+)', block)
    if m:
        return m.group(1)
    # var (...)
    m = re.match(r'var\s*\(', block)
    if m:
        return "<var block>"
    m = re.match(r'var\s+(\w+)', block)
    if m:
        return m.group(1)
    return "<unknown>"

by_name = {}
for d in decls:
    by_name[decl_name(d)] = d

# -----------------------------------------------------------------------------
# Routing table: which declaration goes to which file.
# -----------------------------------------------------------------------------
ROUTES = {
    # testhelpers_test.go
    "testRaftServer":            "testhelpers_test.go",
    "startTestRaftServer":       "testhelpers_test.go",
    "testRaftServer.close":      "testhelpers_test.go",
    "testClusterTLS":            "testhelpers_test.go",
    "newTestClusterTLS":         "testhelpers_test.go",
    "peerIDsExcept":             "testhelpers_test.go",
    "indexOfNodeID":             "testhelpers_test.go",
    "setDeterministicElectionTimeout": "testhelpers_test.go",
    "waitForLeader":             "testhelpers_test.go",
    "waitForCondition":          "testhelpers_test.go",
    "registerRaftService":       "testhelpers_test.go",

    # basic
    "TestGRPCTransportRealRaftNodeRequestVote":  "grpc_raft_cluster_basic_test.go",

    # election
    "TestGRPCTransportThreeNodeRaftElection":    "grpc_raft_cluster_election_test.go",

    # failures
    "TestGRPCTransportFollowerFailure":          "grpc_raft_cluster_follower_failure_test.go",
    "TestGRPCTransportLeaderFailureReElection":  "grpc_raft_cluster_leader_failure_test.go",

    # recovery
    "TestGRPCTransportFollowerRecovery":         "grpc_raft_cluster_follower_recovery_test.go",
    "TestGRPCTransportLeaderRecoveryLogCatchUp": "grpc_raft_cluster_leader_catchup_test.go",

    # WAL recovery
    "TestGRPCTransportFollowerWALRecovery":      "grpc_raft_cluster_wal_recovery_test.go",
    "TestGRPCTransportDivergentFollowerLogRepair": "grpc_raft_cluster_wal_recovery_test.go",

    # restart
    "TestGRPCTransportFullClusterRestartFromWAL": "grpc_raft_cluster_restart_test.go",
}

# -----------------------------------------------------------------------------
# Group declarations by target file, in the original order.
# -----------------------------------------------------------------------------
groups = {}
for d in decls:
    name = decl_name(d)
    target = ROUTES.get(name)
    if target is None:
        print(f"warning: no route for {name!r}; skipping")
        continue
    groups.setdefault(target, []).append(d)

# -----------------------------------------------------------------------------
# Import set — same for every file. Go's compiler and goimports will
# remove unused ones, but including all here guarantees compilation.
# -----------------------------------------------------------------------------
IMPORTS = '''import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)
'''

# -----------------------------------------------------------------------------
# Write each file.
# -----------------------------------------------------------------------------
written = []
for target, blocks in groups.items():
    with open(target, "w") as f:
        f.write("package transport\n\n")
        f.write(IMPORTS)
        f.write("\n")
        for b in blocks:
            f.write(b)
    written.append((target, len(blocks)))

# -----------------------------------------------------------------------------
# Delete the original.
# -----------------------------------------------------------------------------
os.remove(ORIGINAL)

# -----------------------------------------------------------------------------
# Report.
# -----------------------------------------------------------------------------
print("✅ Split complete.\n")
print(f"{'file':<48} {'decls':>5}  {'lines':>6}")
print("-" * 62)
for target, _ in sorted(written):
    with open(target) as f:
        line_count = sum(1 for _ in f)
    print(f"{target:<48} {'-':>5}  {line_count:>6}")
print(f"\nbackup saved at: {BACKUP}")
print("\nnext:")
print("  gofmt -w .")
print("  goimports -w .    # if installed")
print("  go vet ./internal/transport/")
print("  go test ./internal/transport/ -count=1")
print("  go test ./internal/transport/ -count=1 -race")
print("\nwhen green:")
print(f"  rm {BACKUP}")
