package io.astradb.raft.rpc;

/** Represents a Raft RequestVote RPC response. */
public final class RequestVoteResponse {

  private final long term;
  private final boolean voteGranted;

  public RequestVoteResponse(long term, boolean voteGranted) {
    if (term < 0) {
      throw new IllegalArgumentException("Term cannot be negative");
    }

    this.term = term;
    this.voteGranted = voteGranted;
  }

  public long getTerm() {
    return term;
  }

  public boolean isVoteGranted() {
    return voteGranted;
  }
}
