# Multi-Version Concurrency Control (MVCC)

## Overview

AstraDB Storage Engine provides foundational primitives for Multi-Version
Concurrency Control (MVCC).

MVCC allows transactions to observe a consistent point-in-time view of data
while newer versions are created concurrently. Instead of overwriting existing
values, the storage engine maintains multiple versions of each key.

This implementation establishes the single-node MVCC foundation. Distributed
transaction coordination, persistence, replication, and SQL integration are
outside its current scope.

## Components

### TransactionId

`TransactionId` uniquely identifies a transaction using a UUID.

It supports generating new identifiers, constructing identifiers from UUID
instances, and parsing identifiers from strings.

### VersionedValue

`VersionedValue<V>` represents an immutable version of a value.

Each version contains:

- A `HybridTimestamp` identifying when the version was created.
- The stored value.
- A tombstone flag indicating whether the version represents a deletion.

Regular versions require non-null values. Tombstones represent deletion
without storing a value.

### VisibilityChecker

`VisibilityChecker` determines whether a version is visible to a snapshot.

A version is visible when its `HybridTimestamp` is less than or equal to
the snapshot timestamp.

Visibility checking is independent of tombstone handling. A tombstone can
be timestamp-visible, but it represents a deleted key.

### VersionChain

`VersionChain<V>` maintains all versions belonging to a single key.

Versions are maintained in descending timestamp order, with the newest
version first.

The chain supports:

- Adding versions.
- Retrieving the latest version visible to a snapshot.
- Returning an immutable copy of its versions.
- Inspecting its size and whether it is empty.

Duplicate timestamps within a chain are rejected.

When the latest visible version is a tombstone, reads return an empty
result. Earlier snapshots can still observe versions preceding the
deletion.

### Snapshot

`Snapshot` represents a transaction's consistent point-in-time view.

It contains:

- A unique `TransactionId`.
- A `HybridTimestamp` representing its read timestamp.

The snapshot timestamp determines which versions are visible to the
transaction.

### SnapshotManager

`SnapshotManager` tracks currently active transaction snapshots.

It supports:

- Registering snapshots.
- Retrieving snapshots by transaction ID.
- Releasing snapshots when transactions finish.
- Determining the oldest active snapshot timestamp.
- Inspecting the number of active snapshots.

The oldest active timestamp provides a foundation for future MVCC
garbage collection, helping determine which historical versions may
eventually be eligible for removal.

## Read Semantics

For a given key and snapshot:

1. Examine versions from newest to oldest.
2. Find the first version whose timestamp is less than or equal to the
   snapshot timestamp.
3. If that version is a regular value, return the value.
4. If that version is a tombstone, return an empty result.
5. If no version is visible, return an empty result.

This ensures that newer versions do not affect the results of older
snapshots.

## Example

Consider the following version history:

| Timestamp | Value |
|-----------|-------|
| 1000 | `"A"` |
| 2000 | `"B"` |
| 3000 | Tombstone |

Expected reads:

| Snapshot timestamp | Result |
|--------------------|--------|
| 500 | Empty |
| 1000 | `"A"` |
| 1500 | `"A"` |
| 2000 | `"B"` |
| 2500 | `"B"` |
| 3000 | Empty |

A new version created after the tombstone can make the key visible again
to later snapshots without changing earlier snapshot results.

## Thread Safety

`VersionChain` synchronizes operations that mutate or inspect its version
list.

`SnapshotManager` uses a `ConcurrentHashMap` to manage active snapshots.

These primitives provide thread-safe data structure operations. They do
not constitute a complete transaction execution or isolation framework.

## Current Limitations

The current implementation does not provide:

- Transaction commit or rollback coordination.
- Write-ahead logging (WAL).
- Persistent MVCC storage through RocksDB.
- Distributed transactions or Raft replication.
- SQL execution integration.
- Serializable isolation enforcement.
- Automated version garbage collection.

These capabilities can be built on top of the MVCC primitives in future
development.
