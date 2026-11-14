package io.astradb.raft.election;

import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.state.RaftNodeState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/** Tracks the highest log index safely committed by a Raft leader. */
public final class RaftCommitIndexTracker {

  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;
  private final int clusterSize;

  private long commitIndex;

  public RaftCommitIndexTracker(RaftNodeState nodeState, RaftLogStore logStore, int clusterSize) {

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");

    if (clusterSize <= 0) {
      throw new IllegalArgumentException("Cluster size must be positive");
    }

    this.clusterSize = clusterSize;
  }

  public synchronized long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Advances the commit index using replication indexes from every cluster member.
   *
   * <p>The collection must include the leader's own last log index and one match index per
   * follower.
   *
   * @param replicatedIndexes replicated log indexes for all cluster members
   * @return the current commit index
   */
  public synchronized long advanceCommitIndex(Collection<Long> replicatedIndexes) {
    Objects.requireNonNull(replicatedIndexes, "Replicated indexes must not be null");

    if (replicatedIndexes.size() != clusterSize) {
      throw new IllegalArgumentException(
          "Expected replication indexes for exactly " + clusterSize + " cluster members");
    }

    List<Long> sortedIndexes = new ArrayList<>(replicatedIndexes);

    if (sortedIndexes.stream().anyMatch(index -> index == null || index < 0)) {
      throw new IllegalArgumentException("Replication indexes must not be null or negative");
    }

    sortedIndexes.sort((first, second) -> Long.compare(second, first));

    int majorityPosition = clusterSize / 2;
    long candidateCommitIndex = sortedIndexes.get(majorityPosition);

    if (candidateCommitIndex <= commitIndex) {
      return commitIndex;
    }

    long candidateTerm =
        logStore
            .getEntry(candidateCommitIndex)
            .map(entry -> entry.getTerm())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing log entry at candidate commit index " + candidateCommitIndex));

    if (candidateTerm == nodeState.getCurrentTerm()) {
      commitIndex = candidateCommitIndex;
    }

    return commitIndex;
  }
}
