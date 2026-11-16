package io.astradb.raft.rpc;

import java.util.Objects;

/** Request sent by a Raft leader to install a snapshot on a follower. */
public final class InstallSnapshotRequest {

  private final long term;
  private final String leaderId;
  private final long lastIncludedIndex;
  private final long lastIncludedTerm;
  private final byte[] data;

  public InstallSnapshotRequest(
      long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm, byte[] data) {

    if (term <= 0) {
      throw new IllegalArgumentException("Term must be positive");
    }

    this.leaderId = Objects.requireNonNull(leaderId, "Leader ID must not be null");

    if (leaderId.isBlank()) {
      throw new IllegalArgumentException("Leader ID must not be blank");
    }

    if (lastIncludedIndex <= 0) {
      throw new IllegalArgumentException("Last included index must be positive");
    }

    if (lastIncludedTerm <= 0) {
      throw new IllegalArgumentException("Last included term must be positive");
    }

    if (lastIncludedTerm > term) {
      throw new IllegalArgumentException("Last included term cannot exceed current term");
    }

    this.term = term;
    this.lastIncludedIndex = lastIncludedIndex;
    this.lastIncludedTerm = lastIncludedTerm;
    this.data = Objects.requireNonNull(data, "Snapshot data must not be null").clone();
  }

  public long getTerm() {
    return term;
  }

  public String getLeaderId() {
    return leaderId;
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
}
