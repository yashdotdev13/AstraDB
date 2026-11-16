package io.astradb.raft.state;

/** Maintains the monotonically increasing commit index of a Raft node. */
public final class RaftCommitIndex {

  private long commitIndex;

  public RaftCommitIndex() {
    this(0);
  }

  public RaftCommitIndex(long initialCommitIndex) {
    if (initialCommitIndex < 0) {
      throw new IllegalArgumentException("Commit index cannot be negative");
    }

    this.commitIndex = initialCommitIndex;
  }

  public synchronized long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Advances the commit index without allowing it to move backwards.
   *
   * @param newCommitIndex the proposed commit index
   * @return the resulting commit index
   */
  public synchronized long advanceTo(long newCommitIndex) {
    if (newCommitIndex < 0) {
      throw new IllegalArgumentException("Commit index cannot be negative");
    }

    commitIndex = Math.max(commitIndex, newCommitIndex);
    return commitIndex;
  }
}
