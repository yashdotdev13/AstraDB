package io.astradb.raft.election;

import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.state.RaftCommitIndex;
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
  private final RaftCommitIndex commitIndex;

  public RaftCommitIndexTracker(RaftNodeState nodeState, RaftLogStore logStore, int clusterSize) {
    this(nodeState, logStore, clusterSize, new RaftCommitIndex());
  }

  public RaftCommitIndexTracker(
      RaftNodeState nodeState,
      RaftLogStore logStore,
      int clusterSize,
      RaftCommitIndex commitIndex) {

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");

    if (clusterSize <= 0) {
      throw new IllegalArgumentException("Cluster size must be positive");
    }

    this.clusterSize = clusterSize;
    this.commitIndex = Objects.requireNonNull(commitIndex, "Commit index must not be null");
  }

  public long getCommitIndex() {
    return commitIndex.getCommitIndex();
  }

  /**
   * Advances the commit index using replication indexes from every cluster member.
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

    long candidateCommitIndex = sortedIndexes.get(clusterSize / 2);

    if (candidateCommitIndex <= commitIndex.getCommitIndex()) {
      return commitIndex.getCommitIndex();
    }

    long candidateTerm =
        logStore
            .getTerm(candidateCommitIndex)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing log term at candidate commit index " + candidateCommitIndex));

    if (candidateTerm == nodeState.getCurrentTerm()) {
      commitIndex.advanceTo(candidateCommitIndex);
    }

    return commitIndex.getCommitIndex();
  }
}
