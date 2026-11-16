package io.astradb.raft.rpc;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftStateMachineApplier;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Handles incoming Raft AppendEntries RPC requests. */
public final class AppendEntriesHandler {

  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;
  private final ElectionTimeoutResetter timeoutResetter;
  private final RaftCommitIndex commitIndex;
  private final Optional<RaftStateMachineApplier> stateMachineApplier;

  public AppendEntriesHandler(
      RaftNodeState nodeState, RaftLogStore logStore, ElectionTimeoutResetter timeoutResetter) {
    this(nodeState, logStore, timeoutResetter, new RaftCommitIndex());
  }

  public AppendEntriesHandler(
      RaftNodeState nodeState,
      RaftLogStore logStore,
      ElectionTimeoutResetter timeoutResetter,
      RaftCommitIndex commitIndex) {

    this(nodeState, logStore, timeoutResetter, commitIndex, Optional.empty());
  }

  public AppendEntriesHandler(
      RaftNodeState nodeState,
      RaftLogStore logStore,
      ElectionTimeoutResetter timeoutResetter,
      RaftCommitIndex commitIndex,
      RaftStateMachineApplier stateMachineApplier) {

    this(
        nodeState,
        logStore,
        timeoutResetter,
        commitIndex,
        Optional.of(
            Objects.requireNonNull(stateMachineApplier, "State machine applier must not be null")));
  }

  private AppendEntriesHandler(
      RaftNodeState nodeState,
      RaftLogStore logStore,
      ElectionTimeoutResetter timeoutResetter,
      RaftCommitIndex commitIndex,
      Optional<RaftStateMachineApplier> stateMachineApplier) {

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");
    this.timeoutResetter =
        Objects.requireNonNull(timeoutResetter, "Timeout resetter must not be null");
    this.commitIndex = Objects.requireNonNull(commitIndex, "Commit index must not be null");
    this.stateMachineApplier =
        Objects.requireNonNull(stateMachineApplier, "State machine applier must not be null");

    this.stateMachineApplier.ifPresent(
        applier -> {
          if (applier.getCommitIndex() != this.commitIndex) {
            throw new IllegalArgumentException(
                "State machine applier must use the same commit index");
          }
        });
  }

  public long getCommitIndex() {
    return commitIndex.getCommitIndex();
  }

  /** Processes an AppendEntries request from a leader. */
  public synchronized AppendEntriesResponse handle(AppendEntriesRequest request) {
    Objects.requireNonNull(request, "AppendEntries request must not be null");

    // Reject stale leaders and transition to follower for valid leader terms.
    if (!nodeState.acceptLeaderTerm(request.getTerm())) {
      return failureResponse();
    }

    // Valid leader communication resets the election timeout,
    // even when the log consistency check fails.
    timeoutResetter.resetElectionTimeout();

    if (!hasMatchingPreviousEntry(request)) {
      return failureResponse();
    }

    if (!reconcileEntries(request.getEntries())) {
      return failureResponse();
    }

    long newCommitIndex = Math.min(request.getLeaderCommit(), logStore.getLastLogIndex());

    commitIndex.advanceTo(newCommitIndex);

    // Apply newly committed entries after updating the local commit index.
    stateMachineApplier.ifPresent(RaftStateMachineApplier::applyCommittedEntries);

    return new AppendEntriesResponse(nodeState.getCurrentTerm(), true);
  }

  private boolean hasMatchingPreviousEntry(AppendEntriesRequest request) {
    long previousIndex = request.getPreviousLogIndex();

    if (previousIndex == 0) {
      return request.getPreviousLogTerm() == 0;
    }

    if (previousIndex > logStore.getLastLogIndex()) {
      return false;
    }

    Optional<Long> previousTerm = logStore.getTerm(previousIndex);

    return previousTerm.isPresent() && previousTerm.get() == request.getPreviousLogTerm();
  }

  private boolean reconcileEntries(List<RaftLogEntry> incomingEntries) {
    long lastIncludedIndex = logStore.getLastIncludedIndex();

    for (int i = 0; i < incomingEntries.size(); i++) {
      RaftLogEntry incomingEntry = incomingEntries.get(i);

      // Never append or overwrite entries already covered by a snapshot.
      if (incomingEntry.getIndex() <= lastIncludedIndex) {
        return false;
      }

      Optional<RaftLogEntry> existingEntry = logStore.getEntry(incomingEntry.getIndex());

      if (existingEntry.isEmpty()) {
        logStore.appendAll(incomingEntries.subList(i, incomingEntries.size()));
        return true;
      }

      if (existingEntry.get().getTerm() != incomingEntry.getTerm()) {
        // Raft must never overwrite an entry that has already been committed.
        if (incomingEntry.getIndex() <= commitIndex.getCommitIndex()) {
          return false;
        }

        logStore.replaceSuffix(
            incomingEntry.getIndex(), incomingEntries.subList(i, incomingEntries.size()));
        return true;
      }
    }

    return true;
  }

  private AppendEntriesResponse failureResponse() {
    return new AppendEntriesResponse(nodeState.getCurrentTerm(), false);
  }
}
