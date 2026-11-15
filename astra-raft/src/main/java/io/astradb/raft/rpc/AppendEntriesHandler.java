package io.astradb.raft.rpc;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.state.RaftNodeState;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Handles incoming Raft AppendEntries RPC requests. */
public final class AppendEntriesHandler {

  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;
  private final ElectionTimeoutResetter timeoutResetter;

  public AppendEntriesHandler(
      RaftNodeState nodeState, RaftLogStore logStore, ElectionTimeoutResetter timeoutResetter) {

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");
    this.timeoutResetter =
        Objects.requireNonNull(timeoutResetter, "Timeout resetter must not be null");
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

    reconcileEntries(request.getEntries());

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

    Optional<RaftLogEntry> previousEntry = logStore.getEntry(previousIndex);

    return previousEntry.isPresent()
        && previousEntry.get().getTerm() == request.getPreviousLogTerm();
  }

  private void reconcileEntries(List<RaftLogEntry> incomingEntries) {
    for (int i = 0; i < incomingEntries.size(); i++) {
      RaftLogEntry incomingEntry = incomingEntries.get(i);

      Optional<RaftLogEntry> existingEntry = logStore.getEntry(incomingEntry.getIndex());

      if (existingEntry.isEmpty()) {
        logStore.appendAll(incomingEntries.subList(i, incomingEntries.size()));
        return;
      }

      if (existingEntry.get().getTerm() != incomingEntry.getTerm()) {
        logStore.replaceSuffix(
            incomingEntry.getIndex(), incomingEntries.subList(i, incomingEntries.size()));
        return;
      }
    }
  }

  private AppendEntriesResponse failureResponse() {
    return new AppendEntriesResponse(nodeState.getCurrentTerm(), false);
  }
}
