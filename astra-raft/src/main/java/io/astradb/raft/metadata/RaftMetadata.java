package io.astradb.raft.metadata;

import java.util.Optional;

public final class RaftMetadata {

  private final long currentTerm;
  private final String votedFor;

  public RaftMetadata(long currentTerm, String votedFor) {
    if (currentTerm < 0) {
      throw new IllegalArgumentException("Current term cannot be negative");
    }

    if (votedFor != null && votedFor.isBlank()) {
      throw new IllegalArgumentException("Voted-for candidate cannot be blank");
    }

    this.currentTerm = currentTerm;
    this.votedFor = votedFor;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  public Optional<String> getVotedFor() {
    return Optional.ofNullable(votedFor);
  }
}
