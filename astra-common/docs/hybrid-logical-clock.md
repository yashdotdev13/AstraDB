# Hybrid Logical Clock (HLC)

## Overview

AstraDB uses Hybrid Logical Clocks (HLCs) to establish a consistent ordering of events across distributed nodes.

An HLC combines physical wall-clock time with a logical counter. It preserves causal ordering while remaining closely related to physical time.

HLCs are useful for distributed systems because physical clocks alone cannot reliably order concurrent events due to clock skew, synchronization delays, and differences between machines.

## Timestamp Representation

A `HybridTimestamp` consists of two non-negative values:

| Component | Description |
|---|---|
| `physicalTime` | Physical wall-clock time in milliseconds |
| `logicalCounter` | Logical counter used to distinguish events sharing the same physical time |

Timestamps are ordered lexicographically:

1. Compare `physicalTime`.
2. If physical times are equal, compare `logicalCounter`.

This provides deterministic ordering between timestamps.

## Clock Operations

### `now()`

Generates a timestamp representing a new local event.

- If physical time advances beyond the current timestamp, adopt the new physical time and reset the logical counter to zero.
- If physical time remains unchanged or moves backward, increment the logical counter.
- The operation is thread-safe.

### `update(remoteTimestamp)`

Incorporates a timestamp received from another node and generates a subsequent local timestamp.

The resulting timestamp accounts for:
- Current local physical time.
- Current local HLC state.
- Remote physical time and logical counter.

This ensures that an event occurring after the receipt of a remote event receives a timestamp ordered after that remote event.

### `peek()`

Returns the current HLC state without generating a new event or modifying the logical counter.

## Thread Safety

`HybridClock` synchronizes its state-mutating and state-reading operations.

Concurrent calls to `now()` are serialized, ensuring that generated timestamps remain strictly increasing according to HLC ordering.

## Clock Abstraction

AstraDB provides a `Clock` interface to abstract physical time.

- `SystemClock` uses the system wall clock.
- `ManualClock` allows deterministic control of time in tests.

Injecting a clock makes it possible to test clock advancement, backward movement, and logical counter behavior without relying on real-time delays.

## Overflow Handling

The logical counter is represented by a Java `long`.

When it reaches `Long.MAX_VALUE`, the clock advances physical time using checked arithmetic and resets the logical counter to zero.

If physical time itself cannot be incremented without overflowing, an `ArithmeticException` is thrown rather than silently wrapping the timestamp and corrupting ordering.

## Binary Serialization

`HybridTimestamp` supports a fixed-width binary representation of exactly 16 bytes.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 bytes | `physicalTime` |
| 8 | 8 bytes | `logicalCounter` |

Both fields use **big-endian** byte ordering.

The deserializer validates the input length and rejects invalid negative values.

This representation is currently a common-module serialization format. Integration with AstraDB's protocol layer may later define a separate wire-level representation.

## Timestamp Oracle

`TimestampOracle` provides the consumer-facing abstraction for distributed timestamp generation.

`HybridTimestampOracle` is the default implementation backed by `HybridClock`.

Future integration points include:

- Transaction timestamp allocation.
- MVCC version ordering.
- Raft log and event metadata.
- Distributed request coordination.

## Important Limitation

An HLC does **not** guarantee globally synchronized physical time or eliminate clock skew.

Its primary guarantee is causal ordering: if one event happens before another, their HLC timestamps preserve that ordering when clocks are properly updated.

HLC timestamps should not be treated as proof of globally accurate real-world time.