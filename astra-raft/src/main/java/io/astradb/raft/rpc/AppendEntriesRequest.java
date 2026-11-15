package io.astradb.raft.rpc;

import io.astradb.raft.log.RaftLogEntry;
import java.util.List;
import java.util.Objects;

/** Represents a Raft AppendEntries RPC request. */
public final class AppendEntriesRequest {

  private final long term;
  private final String leaderId;
  private final long previousLogIndex;
  private final long previousLogTerm;
  private final List<RaftLogEntry> entries;
  private final long leaderCommit;

  public AppendEntriesRequest(
      long term,
      String leaderId,
      long previousLogIndex,
      long previousLogTerm,
      List<RaftLogEntry> entries,
      long leaderCommit) {

    if (term < 0) {
      throw new IllegalArgumentException("Term cannot be negative");
    }

    this.leaderId = Objects.requireNonNull(leaderId, "Leader ID must not be null");

    if (leaderId.isBlank()) {
      throw new IllegalArgumentException("Leader ID must not be blank");
    }

    if (previousLogIndex < 0 || previousLogTerm < 0) {
      throw new IllegalArgumentException("Previous log index and term cannot be negative");
    }

    if (leaderCommit < 0) {
      throw new IllegalArgumentException("Leader commit index cannot be negative");
    }

    this.entries = List.copyOf(Objects.requireNonNull(entries, "Entries must not be null"));

    long expectedIndex = previousLogIndex;

    for (RaftLogEntry entry : this.entries) {
      if (expectedIndex == Long.MAX_VALUE) {
        throw new IllegalArgumentException("Log index overflow");
      }

      expectedIndex++;

      if (entry.getIndex() != expectedIndex) {
        throw new IllegalArgumentException(
            "Expected log entry at index " + expectedIndex + " but received " + entry.getIndex());
      }
    }

    this.term = term;
    this.previousLogIndex = previousLogIndex;
    this.previousLogTerm = previousLogTerm;
    this.leaderCommit = leaderCommit;
  }

  public long getTerm() {
    return term;
  }

  public String getLeaderId() {
    return leaderId;
  }

  public long getPreviousLogIndex() {
    return previousLogIndex;
  }

  public long getPreviousLogTerm() {
    return previousLogTerm;
  }

  public List<RaftLogEntry> getEntries() {
    return entries;
  }

  public long getLeaderCommit() {
    return leaderCommit;
  }
}
