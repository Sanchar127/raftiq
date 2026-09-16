# Persistence Overview

RaftIQ's durability guarantees live entirely in `internal/storage`, behind
the `Storage` interface consumed by `RaftNode`. This page is the overview;
see [`storage/overview.md`](storage/overview.md),
[`storage/wal.md`](storage/wal.md), and
[`storage/snapshots.md`](storage/snapshots.md) for depth.

## Why this exists

A Raft node must not "forget" its current term, its vote in that term, or
any log entry it has acknowledged to a leader — doing so risks violating
election safety or the log-matching property after a crash and restart.
`Storage.SaveState`/`AppendEntries`/`Sync` exist to make those guarantees
survive a process crash.

## What's persisted

Per `model.PersistentState` and the `Storage` interface
(`internal/storage/storage.go`):

- **Persistent state**: current term, voted-for candidate, and the
  membership configuration (including any in-flight joint configuration).
- **Log entries**: every `LogEntry` accepted into the log, appended via
  `AppendEntries`, potentially rewritten via `ReplaceSuffix` (only ever
  above the commit index — see the Raft safety rules in
  [`CONTRIBUTING.md`](../CONTRIBUTING.md)).
- **Snapshots**: a `model.Snapshot` capturing state-machine data as of a
  given `LastIncludedIndex`/`LastIncludedTerm`, used to compact the log.

## Two implementations of `Storage`

- **`WALStorage`** (`internal/storage/wal.go`) — the durable, disk-backed
  implementation used by `cmd/raftiq`. See [`storage/wal.md`](storage/wal.md).
- **`MemoryStorage`** (`internal/storage/memory.go`) — an in-memory,
  non-durable implementation with the same interface, used in tests and
  anywhere durability isn't required.

## Recovery, in one sentence

On `OpenWAL`, RaftIQ replays every well-formed record in order and, if the
file ends mid-write (a crash during a write), truncates that one
incomplete trailing record rather than failing to start or losing earlier,
valid records. A corrupted record that is *not* the trailing one (e.g. a
bad CRC in the middle of the file) is a hard failure (`ErrWALCorrupt`), not
silently skipped — see [`storage/wal.md`](storage/wal.md#recovery) for the
exact logic.

## Sync / durability semantics

Writes go through `os` file APIs with `O_APPEND`; `Sync()` calls the
platform sync primitive to force data to stable storage. `SaveState` and
`AppendEntries` in `WALStorage` write and then rely on the caller (or an
internal call) to `Sync()` before treating the write as durable — check
`internal/storage/wal.go` directly for exactly which calls sync inline
versus which defer to an explicit `Sync()` call, since getting this wrong
either costs unnecessary latency or unnecessary risk.

## Failure modes handled today

| Failure | Handling |
|---|---|
| Crash mid-write (partial trailing record) | Detected via `io.ErrUnexpectedEOF` in `decodeRecord`; tail is truncated on next `OpenWAL`. |
| Bit-level corruption (bad CRC) mid-file | `ErrWALCorrupt` returned; the file is not automatically repaired. |
| Disk full on write | `ErrWALDiskFull` sentinel exists and is returned on write failure; **not** covered by a dedicated `ENOSPC` chaos test at the time of this audit. |
| Concurrent access to the same `WALStorage` | Guarded by `WALStorage.mu` (`sync.RWMutex`); not safe to open the same file from two processes. |

See [`AUDIT.md`](AUDIT.md) for the full status table.
