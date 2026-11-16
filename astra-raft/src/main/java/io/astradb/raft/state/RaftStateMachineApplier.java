package io.astradb.raft.state;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.snapshot.RaftSnapshot;
import java.util.Objects;
import java.util.Optional;

/** Applies committed Raft log entries to a state machine in log order. */
public final class RaftStateMachineApplier {

  private final RaftLogStore logStore;
  private final RaftCommitIndex commitIndex;
  private final RaftStateMachine stateMachine;

  private long lastApplied;

  public RaftStateMachineApplier(
      RaftLogStore logStore, RaftCommitIndex commitIndex, RaftStateMachine stateMachine) {

    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");
    this.commitIndex = Objects.requireNonNull(commitIndex, "Commit index must not be null");
    this.stateMachine = Objects.requireNonNull(stateMachine, "State machine must not be null");
  }

  public synchronized long getLastApplied() {
    return lastApplied;
  }

  /** Returns the commit-index instance used by this applier. */
  public RaftCommitIndex getCommitIndex() {
    return commitIndex;
  }

  /**
   * Applies all committed entries that have not yet been applied.
   *
   * @return the latest successfully applied log index
   */
  public synchronized long applyCommittedEntries() {
    while (lastApplied < commitIndex.getCommitIndex()) {
      long nextIndex = Math.incrementExact(lastApplied);

      Optional<RaftLogEntry> entry = logStore.getEntry(nextIndex);

      RaftLogEntry committedEntry =
          entry.orElseThrow(
              () -> new IllegalStateException("Missing committed log entry at index " + nextIndex));

      stateMachine.apply(nextIndex, committedEntry.getPayload());

      // Advance only after successful application.
      lastApplied = nextIndex;
    }

    return lastApplied;
  }

  /**
   * Restores the state machine from a snapshot.
   *
   * @param snapshot snapshot to install
   * @return the snapshot's last included index
   */
  public synchronized long installSnapshot(RaftSnapshot snapshot) {
    Objects.requireNonNull(snapshot, "Snapshot must not be null");

    long snapshotIndex = snapshot.getLastIncludedIndex();

    if (snapshotIndex < lastApplied) {
      throw new IllegalArgumentException("Snapshot index cannot be behind the last applied index");
    }

    // Restore application state before advancing Raft progress.
    stateMachine.restore(snapshot.getData());

    commitIndex.advanceTo(snapshotIndex);
    lastApplied = snapshotIndex;

    return lastApplied;
  }
}
