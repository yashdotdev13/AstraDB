package io.astradb.raft.rpc;

/** Response returned by a follower after processing an InstallSnapshot request. */
public final class InstallSnapshotResponse {

  private final long term;
  private final boolean success;

  public InstallSnapshotResponse(long term, boolean success) {
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
