package io.astradb.raft.rpc;

import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.snapshot.RaftSnapshot;
import io.astradb.raft.snapshot.RaftSnapshotStore;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftStateMachineApplier;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Handles incoming Raft InstallSnapshot RPC requests. */
public final class InstallSnapshotHandler {

  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;
  private final RaftSnapshotStore snapshotStore;
  private final ElectionTimeoutResetter timeoutResetter;
  private final RaftStateMachineApplier stateMachineApplier;

  public InstallSnapshotHandler(
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftSnapshotStore snapshotStore,
      ElectionTimeoutResetter timeoutResetter,
      RaftStateMachineApplier stateMachineApplier) {

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");
    this.snapshotStore = Objects.requireNonNull(snapshotStore, "Snapshot store must not be null");
    this.timeoutResetter =
        Objects.requireNonNull(timeoutResetter, "Timeout resetter must not be null");
    this.stateMachineApplier =
        Objects.requireNonNull(stateMachineApplier, "State machine applier must not be null");
  }

  /** Processes an InstallSnapshot request from a leader. */
  public synchronized InstallSnapshotResponse handle(InstallSnapshotRequest request) {
    Objects.requireNonNull(request, "InstallSnapshot request must not be null");

    // Reject stale leaders.
    if (!nodeState.acceptLeaderTerm(request.getTerm())) {
      return response(false);
    }

    // Valid leader communication resets the election timeout.
    timeoutResetter.resetElectionTimeout();

    long snapshotIndex = request.getLastIncludedIndex();
    long snapshotTerm = request.getLastIncludedTerm();

    Optional<RaftSnapshot> persistedSnapshot = snapshotStore.load();

    // Never replace a newer persisted snapshot with an older one.
    if (persistedSnapshot.isPresent()) {
      RaftSnapshot currentSnapshot = persistedSnapshot.get();
      long currentSnapshotIndex = currentSnapshot.getLastIncludedIndex();

      if (snapshotIndex < currentSnapshotIndex) {
        return response(false);
      }

      // A snapshot at the same index is successful only if its history matches.
      if (snapshotIndex == currentSnapshotIndex) {
        boolean identical =
            snapshotTerm == currentSnapshot.getLastIncludedTerm()
                && Arrays.equals(request.getData(), currentSnapshot.getData());

        return response(identical);
      }
    }

    // Check whether this snapshot is already represented by committed state.
    long commitIndex = stateMachineApplier.getCommitIndex().getCommitIndex();

    if (snapshotIndex <= commitIndex) {
      Optional<Long> localTerm = logStore.getTerm(snapshotIndex);

      return response(localTerm.isPresent() && localTerm.get() == snapshotTerm);
    }

    // Ignore snapshots that do not advance the installed log boundary.
    if (snapshotIndex <= logStore.getLastIncludedIndex()) {
      return response(false);
    }

    RaftSnapshot snapshot =
        new RaftSnapshot(snapshotIndex, snapshotTerm, request.getData(), Instant.now());

    // Persist the snapshot before changing the log boundary.
    snapshotStore.save(snapshot);

    // Restore application state and advance commit/applied progress.
    stateMachineApplier.installSnapshot(snapshot);

    // Install the durable log boundary, preserving a matching suffix when possible.
    logStore.installSnapshotBoundary(snapshotIndex, snapshotTerm);

    return response(true);
  }

  private InstallSnapshotResponse response(boolean success) {
    return new InstallSnapshotResponse(nodeState.getCurrentTerm(), success);
  }
}
