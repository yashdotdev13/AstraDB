package io.astradb.raft.log;

import java.util.Objects;

/**
 * Represents an entry in the Raft replicated log.
 *
 * @param index position of the entry in the log
 * @param term Raft term in which the entry was created
 * @param payload serialized command or data
 */
public final class RaftLogEntry {

  private final long index;
  private final long term;
  private final byte[] payload;

  public RaftLogEntry(long index, long term, byte[] payload) {
    if (index <= 0) {
      throw new IllegalArgumentException("Log index must be positive");
    }

    if (term <= 0) {
      throw new IllegalArgumentException("Raft term must be positive");
    }

    this.index = index;
    this.term = term;
    this.payload = Objects.requireNonNull(payload, "Payload must not be null").clone();
  }

  public long getIndex() {
    return index;
  }

  public long getTerm() {
    return term;
  }

  public byte[] getPayload() {
    return payload.clone();
  }
}
