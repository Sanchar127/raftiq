# Storage: WAL

`internal/storage/wal.go`, `WALStorage`. This is the durable
implementation of the `Storage` interface used by `cmd/raftiq`.

## Record format

Every record is length-prefixed and checksummed:

| Field | Size | Notes |
|---|---:|---|
| Record type | 1 byte | One of the constants below |
| Payload length | 4 bytes | `uint32`, big-endian |
| Payload | N bytes | Type-specific encoding, see below (max 16 MiB, `maxRecordPayloadSize`) |
| CRC32 | 4 bytes | IEEE CRC32 over `type ‖ length ‖ payload`, big-endian |

(`recordHeaderSize = 1 + 4`, `recordFooterSize = 4` in `wal.go`.)

Record types (`wal.go` constants):

| Constant | Value | Payload |
|---|---:|---|
| `recordState` | 1 | Encoded `model.PersistentState` (`encodeStateRecord`) |
| `recordEntries` | 2 | One or more appended `model.LogEntry` (`encodeEntriesRecord`) |
| `recordSnapshot` | 3 | Encoded `model.Snapshot` (`encodeSnapshotPayload`) |
| `recordReplaceSuffix` | 4 | A suffix-replacement operation (`encodeReplaceSuffixRecord`) — used when a follower's log conflicts with a leader's and must be rewritten from some index onward |

`PersistentState` payloads additionally carry a membership header —
`stateMembershipMagic = 0x52414654` (ASCII `"RAFT"`) and
`stateMembershipVersion = 1` — used by `encodeMembership`/`decodeMembership`
to version the membership/configuration encoding independently of the
outer record format.

## Recovery algorithm

`OpenWAL(path)` opens the file (`O_RDWR|O_CREATE|O_APPEND`) and calls
`recover()`, which:

1. Seeks to the start of the file.
2. Repeatedly calls `decodeRecord` (reads type, length, payload, CRC;
   verifies the CRC).
3. On a **clean EOF** (nothing left to read, aligned on a record boundary)
   — recovery is done; everything read so far is valid.
4. On `io.ErrUnexpectedEOF` (the file ends partway through a record —
   e.g. the process crashed mid-`Write`) — recovery **truncates the file**
   back to the last known-good record boundary (`validOffset`) and stops.
   This is the "partial final record caused by an interrupted write" case:
   it is repaired automatically, not treated as fatal.
5. On any **other decode error** (a CRC mismatch, or a record type it
   doesn't recognize) — this is treated as genuine corruption
   (`ErrWALCorrupt`-class failure) and `OpenWAL` fails. Unlike the
   partial-tail case, RaftIQ does **not** attempt to skip past corrupted
   bytes in the middle of the file and keep going — a bad CRC on anything
   but the very last record means the file cannot be trusted past that
   point, and startup fails loudly rather than silently discarding data.
6. Each successfully decoded record is applied to in-memory
   `state`/`entries`/`snapshot` fields in the order it appears, so the
   final in-memory state after recovery reflects the sequence of writes as
   they actually happened (including any `recordReplaceSuffix` rewrites).

Tests: `TestWALStorageRecoversValidRecordsBeforeTruncatedTail` (case 4),
`TestWALStorageRejectsCorruptedRecordOnRecovery` (case 5),
`TestWALStorageReplaceSuffixTruncatesOnly` (verifies suffix replacement
only removes what it should, not more).

## Suffix replacement

`ReplaceSuffix(from, entries)` is how a follower's log is corrected when a
leader's `AppendEntries` reveals a conflict — entries at and after `from`
are discarded and replaced with the leader's version. This is written as
its own record type (`recordReplaceSuffix`) rather than by directly
editing earlier records in place, keeping the WAL append-only.
`replaceSuffixCopy` in `wal.go` implements the actual entry-slice surgery
in memory; the persisted record captures the same operation for replay on
recovery.

## Sync semantics

`Sync()` calls the OS-level file sync. `WALStorage` tracks a `syncFn`
field (overridable, used by tests to simulate sync failures) rather than
calling `file.Sync()` unconditionally inline — check the call sites of
`Sync()` in `wal.go` for exactly which write paths sync before returning
versus which leave it to an explicit caller-driven `Sync()`.

## Errors

| Sentinel | Meaning |
|---|---|
| `ErrInvalidLog` | Structural log-sequencing problem detected during validation |
| `ErrWALCorrupt` | A record failed CRC/decoding in a way recovery can't repair |
| `ErrWALVersion` | Membership payload version not understood |
| `ErrWALDiskFull` | Write failed due to insufficient disk space |
| `ErrClosedStorage` | Operation attempted after `Close()` |

## Concurrency

`WALStorage.mu` (a `sync.RWMutex`) guards all state; reads (`LoadState`,
`LoadEntries`, `LoadSnapshot`) take the read lock, writes take the write
lock. It is not safe to share one `WALStorage`/file across two OS
processes.
