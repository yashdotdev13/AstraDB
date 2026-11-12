package io.astradb.raft.rpc;

import java.util.Objects;

/** Represents a Raft RequestVote RPC request. */
public final class RequestVoteRequest {

  private final long term;
  private final String candidateId;
  private final long lastLogIndex;
  private final long lastLogTerm;

  public RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    if (term < 0) {
      throw new IllegalArgumentException("Term cannot be negative");
    }

    this.candidateId = Objects.requireNonNull(candidateId, "Candidate ID must not be null");

    if (candidateId.isBlank()) {
      throw new IllegalArgumentException("Candidate ID must not be blank");
    }

    if (lastLogIndex < 0 || lastLogTerm < 0) {
      throw new IllegalArgumentException("Log index and term cannot be negative");
    }

    this.term = term;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
  }

  public long getTerm() {
    return term;
  }

  public String getCandidateId() {
    return candidateId;
  }

  public long getLastLogIndex() {
    return lastLogIndex;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }
}
