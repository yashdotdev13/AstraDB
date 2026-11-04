# Write-Ahead Log (WAL)

## Overview

AstraDB's Write-Ahead Log (WAL) provides a durable, append-only record of transaction operations. It is the foundation for crash recovery and ensures that committed transaction operations can be reconstructed after a restart.

The WAL currently supports single-node persistence and recovery. Replication, checkpointing, and log compaction are outside its current scope.

## Architecture

The WAL implementation consists of the following components:

- **LogSequenceNumber:** Identifies the sequential position of each record in the WAL.
- **WalRecordType:** Defines transaction lifecycle and operation types.
- **WalRecord:** Immutable representation of a WAL operation.
- **WalRecordSerializer:** Encodes and decodes WAL records using a binary format with CRC32 checksums.
- **WalWriter:** Appends records to the WAL file and forces commit and abort records to stable storage.
- **WalReader:** Reads and deserializes WAL records sequentially.
- **WalManager:** Coordinates WAL operations and assigns LSNs.
- **WalRecovery:** Reconstructs committed transaction operations from the WAL.

## Record Types

| Record | Description |
|---|---|
| `BEGIN` | Marks the beginning of a transaction |
| `PUT` | Records a key-value write |
| `DELETE` | Records a key deletion |
| `COMMIT` | Marks a transaction as committed |
| `ABORT` | Marks a transaction as aborted |

Each record contains a log sequence number, transaction identifier, record type, hybrid timestamp, and optional key-value payload.

## Binary Record Format

The serialized record contains:

| Field | Description |
|---|---|
| Magic number | Identifies the AstraDB WAL format |
| Version | WAL serialization format version |
| LSN | Record sequence number |
| Transaction ID | UUID identifying the transaction |
| Record type | Operation or transaction lifecycle type |
| Hybrid timestamp | Physical time and logical counter |
| Key | Length-prefixed key bytes |
| Value | Length-prefixed value bytes |
| CRC32 | Checksum used to detect corruption |

Each serialized record is preceded by a four-byte length prefix, allowing the reader to identify record boundaries.

## Durability

The WAL writer uses Java NIO `FileChannel` for append operations.

- Records are appended sequentially.
- Commit and abort records trigger a force operation.
- Explicit flush operations force buffered data to storage.
- LSNs continue from the last persisted record when the WAL is reopened.

## Recovery

During recovery, WAL records are grouped by transaction identifier.

- Committed transactions are eligible for recovery.
- Aborted transactions are excluded.
- Transactions without a commit record are treated as incomplete and excluded.
- Recovered records preserve their original WAL order.
- Transaction lifecycle markers (`COMMIT` and `ABORT`) are not returned as recovered operations.

## Limitations

The current WAL implementation does not yet include:

- Checkpointing
- WAL segment rotation
- Log compaction
- Replicated WAL
- RocksDB integration
- Distributed recovery coordination

These capabilities will be introduced in subsequent AstraDB development stages.
