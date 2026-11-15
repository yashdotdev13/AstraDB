package io.astradb.raft.election;

/** Tracks the log replication progress of a follower from the leader's perspective. */
public final class RaftPeerReplicationState {

  private long nextIndex;
  private long matchIndex;

  public RaftPeerReplicationState(long initialNextIndex) {
    if (initialNextIndex <= 0) {
      throw new IllegalArgumentException("Initial next index must be positive");
    }

    this.nextIndex = initialNextIndex;
    this.matchIndex = 0;
  }

  public synchronized long getNextIndex() {
    return nextIndex;
  }

  public synchronized long getMatchIndex() {
    return matchIndex;
  }

  /** Records successful replication through the supplied log index. */
  public synchronized void recordSuccess(long replicatedThroughIndex) {
    if (replicatedThroughIndex < 0) {
      throw new IllegalArgumentException("Replicated index cannot be negative");
    }

    matchIndex = Math.max(matchIndex, replicatedThroughIndex);
    nextIndex = Math.addExact(matchIndex, 1);
  }

  /**
   * Moves nextIndex backwards after a failed request.
   *
   * <p>A stale failure must not undo progress made by a newer request.
   */
  public synchronized void recordFailure(long attemptedNextIndex) {
    if (attemptedNextIndex <= 0) {
      throw new IllegalArgumentException("Attempted next index must be positive");
    }

    if (nextIndex != attemptedNextIndex) {
      return;
    }

    long minimumNextIndex = matchIndex + 1;

    if (nextIndex > minimumNextIndex) {
      nextIndex--;
    }
  }
}
