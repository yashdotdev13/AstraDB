package io.astradb.raft.election;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Sends periodic AppendEntries RPCs and replicates log entries to Raft followers. */
public final class RaftHeartbeatManager implements AutoCloseable {

  private final String localNodeId;
  private final Set<String> peerIds;
  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;
  private final RaftPeerClient peerClient;
  private final ScheduledExecutorService scheduler;
  private final long heartbeatIntervalMillis;

  private final Map<String, RaftPeerReplicationState> replicationStates = new HashMap<>();

  private ScheduledFuture<?> heartbeatTask;
  private boolean started;
  private boolean closed;

  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis) {

    this.localNodeId = Objects.requireNonNull(localNodeId, "Local node ID must not be null");

    if (localNodeId.isBlank()) {
      throw new IllegalArgumentException("Local node ID must not be blank");
    }

    Objects.requireNonNull(peerIds, "Peer IDs must not be null");

    this.peerIds = Set.copyOf(peerIds);

    if (this.peerIds.contains(localNodeId)) {
      throw new IllegalArgumentException("Peer IDs must not contain the local node");
    }

    if (this.peerIds.stream().anyMatch(String::isBlank)) {
      throw new IllegalArgumentException("Peer IDs must not contain blank values");
    }

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");
    this.peerClient = Objects.requireNonNull(peerClient, "Peer client must not be null");
    this.scheduler = Objects.requireNonNull(scheduler, "Scheduler must not be null");

    if (heartbeatIntervalMillis <= 0) {
      throw new IllegalArgumentException("Heartbeat interval must be positive");
    }

    this.heartbeatIntervalMillis = heartbeatIntervalMillis;
  }

  /** Starts periodic AppendEntries transmission. The node must already be a leader. */
  public synchronized void start() {
    if (closed) {
      throw new IllegalStateException("Heartbeat manager is closed");
    }

    if (started) {
      return;
    }

    if (nodeState.getRole() != RaftRole.LEADER) {
      throw new IllegalStateException("Only a leader can start heartbeats");
    }

    initializeReplicationStates();
    started = true;
    scheduleNextHeartbeat();
  }

  private void initializeReplicationStates() {
    replicationStates.clear();

    long initialNextIndex = Math.addExact(logStore.getLastLogIndex(), 1);

    for (String peerId : peerIds) {
      replicationStates.put(peerId, new RaftPeerReplicationState(initialNextIndex));
    }
  }

  /** Sends AppendEntries RPCs immediately to all configured peers. */
  public synchronized void sendHeartbeats() {
    if (closed || !started || nodeState.getRole() != RaftRole.LEADER) {
      stopScheduling();
      return;
    }

    long term = nodeState.getCurrentTerm();

    for (String peerId : peerIds) {
      try {
        sendAppendEntries(peerId, term);
      } catch (RuntimeException exception) {
        // A failed peer must not prevent replication to other peers.
      }
    }
  }

  private void sendAppendEntries(String peerId, long term) {
    RaftPeerReplicationState replicationState = replicationStates.get(peerId);

    if (replicationState == null) {
      return;
    }

    long nextIndex = replicationState.getNextIndex();
    long previousLogIndex = nextIndex - 1;
    long lastLogIndex = logStore.getLastLogIndex();

    long previousLogTerm =
        previousLogIndex == 0
            ? 0
            : logStore
                .getEntry(previousLogIndex)
                .map(RaftLogEntry::getTerm)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Missing previous log entry at index " + previousLogIndex));

    List<RaftLogEntry> entries =
        nextIndex > lastLogIndex ? List.of() : logStore.getEntries(nextIndex, lastLogIndex);

    long replicatedThroughIndex =
        entries.isEmpty() ? previousLogIndex : entries.get(entries.size() - 1).getIndex();

    AppendEntriesRequest request =
        new AppendEntriesRequest(term, localNodeId, previousLogIndex, previousLogTerm, entries, 0);

    peerClient
        .appendEntries(peerId, request)
        .whenComplete(
            (response, error) ->
                handleResponse(peerId, term, nextIndex, replicatedThroughIndex, response, error));
  }

  private synchronized void handleResponse(
      String peerId,
      long requestTerm,
      long attemptedNextIndex,
      long replicatedThroughIndex,
      AppendEntriesResponse response,
      Throwable error) {

    if (closed || !started || error != null || response == null) {
      return;
    }

    if (response.getTerm() > nodeState.getCurrentTerm()) {
      nodeState.advanceTermIfHigher(response.getTerm());
      stopScheduling();
      return;
    }

    if (requestTerm != nodeState.getCurrentTerm()) {
      return;
    }

    RaftPeerReplicationState replicationState = replicationStates.get(peerId);

    if (replicationState == null) {
      return;
    }

    if (response.isSuccess()) {
      replicationState.recordSuccess(replicatedThroughIndex);
    } else {
      replicationState.recordFailure(attemptedNextIndex);
    }
  }

  private synchronized void scheduleNextHeartbeat() {
    if (!started || closed || nodeState.getRole() != RaftRole.LEADER) {
      stopScheduling();
      return;
    }

    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
    }

    heartbeatTask =
        scheduler.schedule(
            this::runScheduledHeartbeat, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
  }

  private void runScheduledHeartbeat() {
    synchronized (this) {
      heartbeatTask = null;

      if (!started || closed || nodeState.getRole() != RaftRole.LEADER) {
        stopScheduling();
        return;
      }
    }

    sendHeartbeats();

    synchronized (this) {
      scheduleNextHeartbeat();
    }
  }

  private synchronized void stopScheduling() {
    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
      heartbeatTask = null;
    }
  }

  /** Stops periodic AppendEntries transmission. */
  public synchronized void stop() {
    started = false;
    stopScheduling();
  }

  /** Closes the manager without shutting down the caller-owned scheduler. */
  @Override
  public synchronized void close() {
    stop();
    closed = true;
  }
}
