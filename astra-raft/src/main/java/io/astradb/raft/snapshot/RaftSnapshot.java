package io.astradb.raft.snapshot;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/** Represents a point-in-time snapshot of a Raft state machine. */
public final class RaftSnapshot {

  private final long lastIncludedIndex;
  private final long lastIncludedTerm;
  private final byte[] data;
  private final Instant createdAt;

  public RaftSnapshot(
      long lastIncludedIndex, long lastIncludedTerm, byte[] data, Instant createdAt) {

    if (lastIncludedIndex <= 0) {
      throw new IllegalArgumentException("Last included index must be positive");
    }

    if (lastIncludedTerm <= 0) {
      throw new IllegalArgumentException("Last included term must be positive");
    }

    this.lastIncludedIndex = lastIncludedIndex;
    this.lastIncludedTerm = lastIncludedTerm;
    this.data = Objects.requireNonNull(data, "Snapshot data must not be null").clone();
    this.createdAt = Objects.requireNonNull(createdAt, "Creation time must not be null");
  }

  public long getLastIncludedIndex() {
    return lastIncludedIndex;
  }

  public long getLastIncludedTerm() {
    return lastIncludedTerm;
  }

  public byte[] getData() {
    return data.clone();
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }

    if (!(object instanceof RaftSnapshot)) {
      return false;
    }

    RaftSnapshot that = (RaftSnapshot) object;

    return lastIncludedIndex == that.lastIncludedIndex
        && lastIncludedTerm == that.lastIncludedTerm
        && Arrays.equals(data, that.data)
        && createdAt.equals(that.createdAt);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(lastIncludedIndex, lastIncludedTerm, createdAt);
    return 31 * result + Arrays.hashCode(data);
  }
}
