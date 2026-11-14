package io.astradb.raft.rpc;

/** Represents a Raft AppendEntries RPC response. */
public final class AppendEntriesResponse {

  private final long term;
  private final boolean success;

  public AppendEntriesResponse(long term, boolean success) {
    if (term < 0) {
      throw new IllegalArgumentException("Term cannot be negative");
    }

    this.term = term;
    this.success = success;
  }

  public long getTerm() {
    return term;
  }

  public boolean isSuccess() {
    return success;
  }
}
