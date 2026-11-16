package io.astradb.raft.election;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.rpc.InstallSnapshotRequest;
import io.astradb.raft.rpc.InstallSnapshotResponse;
import io.astradb.raft.snapshot.RaftSnapshot;
import io.astradb.raft.snapshot.RaftSnapshotStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import io.astradb.raft.state.RaftStateMachineApplier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
  private final RaftCommitIndexTracker commitIndexTracker;
  private final Optional<RaftStateMachineApplier> stateMachineApplier;
  private final Optional<RaftSnapshotStore> snapshotStore;

  private final Map<String, RaftPeerReplicationState> replicationStates = new HashMap<>();

  private final Map<String, Object> replicationRequestsInFlight = new HashMap<>();

  private ScheduledFuture<?> heartbeatTask;
  private boolean started;
  private boolean closed;

  /**
   * Creates a heartbeat manager with a new local commit-index instance.
   *
   * <p>Use a constructor accepting a shared RaftCommitIndex when other components on this node need
   * to observe the same commit index.
   */
  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis) {

    this(
        localNodeId,
        peerIds,
        nodeState,
        logStore,
        peerClient,
        scheduler,
        heartbeatIntervalMillis,
        new RaftCommitIndex());
  }

  /** Creates a heartbeat manager using the supplied shared commit index. */
  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis,
      RaftCommitIndex commitIndex) {

    this(
        localNodeId,
        peerIds,
        nodeState,
        logStore,
        peerClient,
        scheduler,
        heartbeatIntervalMillis,
        commitIndex,
        Optional.empty(),
        Optional.empty());
  }

  /** Creates a heartbeat manager with snapshot replication enabled. */
  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis,
      RaftSnapshotStore snapshotStore) {

    this(
        localNodeId,
        peerIds,
        nodeState,
        logStore,
        peerClient,
        scheduler,
        heartbeatIntervalMillis,
        new RaftCommitIndex(),
        Optional.empty(),
        Optional.of(Objects.requireNonNull(snapshotStore, "Snapshot store must not be null")));
  }

  /** Creates a heartbeat manager with shared commit index and snapshot replication. */
  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis,
      RaftCommitIndex commitIndex,
      RaftSnapshotStore snapshotStore) {

    this(
        localNodeId,
        peerIds,
        nodeState,
        logStore,
        peerClient,
        scheduler,
        heartbeatIntervalMillis,
        commitIndex,
        Optional.empty(),
        Optional.of(Objects.requireNonNull(snapshotStore, "Snapshot store must not be null")));
  }

  /**
   * Creates a heartbeat manager with shared commit-index and state-machine application.
   *
   * <p>The supplied applier must use the exact same commit-index instance.
   */
  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis,
      RaftCommitIndex commitIndex,
      RaftStateMachineApplier stateMachineApplier) {

    this(
        localNodeId,
        peerIds,
        nodeState,
        logStore,
        peerClient,
        scheduler,
        heartbeatIntervalMillis,
        commitIndex,
        Optional.of(
            Objects.requireNonNull(stateMachineApplier, "State machine applier must not be null")),
        Optional.empty());
  }

  /**
   * Creates a heartbeat manager with shared commit index, state-machine application and snapshots.
   *
   * <p>The supplied applier must use the exact same commit-index instance.
   */
  public RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis,
      RaftCommitIndex commitIndex,
      RaftStateMachineApplier stateMachineApplier,
      RaftSnapshotStore snapshotStore) {

    this(
        localNodeId,
        peerIds,
        nodeState,
        logStore,
        peerClient,
        scheduler,
        heartbeatIntervalMillis,
        commitIndex,
        Optional.of(
            Objects.requireNonNull(stateMachineApplier, "State machine applier must not be null")),
        Optional.of(Objects.requireNonNull(snapshotStore, "Snapshot store must not be null")));
  }

  private RaftHeartbeatManager(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ScheduledExecutorService scheduler,
      long heartbeatIntervalMillis,
      RaftCommitIndex commitIndex,
      Optional<RaftStateMachineApplier> stateMachineApplier,
      Optional<RaftSnapshotStore> snapshotStore) {

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

    RaftCommitIndex sharedCommitIndex =
        Objects.requireNonNull(commitIndex, "Commit index must not be null");

    this.stateMachineApplier =
        Objects.requireNonNull(stateMachineApplier, "State machine applier must not be null");

    this.snapshotStore =
        Objects.requireNonNull(snapshotStore, "Snapshot store optional must not be null");

    this.stateMachineApplier.ifPresent(
        applier -> {
          if (applier.getCommitIndex() != sharedCommitIndex) {
            throw new IllegalArgumentException(
                "State machine applier must use the same commit index");
          }
        });

    this.commitIndexTracker =
        new RaftCommitIndexTracker(
            this.nodeState, this.logStore, this.peerIds.size() + 1, sharedCommitIndex);
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

  /** Sends AppendEntries or InstallSnapshot RPCs immediately to all configured peers. */
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
    if (replicationRequestsInFlight.containsKey(peerId)) {
      return;
    }

    RaftPeerReplicationState replicationState = replicationStates.get(peerId);

    if (replicationState == null) {
      return;
    }

    long nextIndex = replicationState.getNextIndex();
    long lastIncludedIndex = logStore.getLastIncludedIndex();

    if (nextIndex <= lastIncludedIndex) {
      sendInstallSnapshot(peerId, term);
      return;
    }

    long previousLogIndex = nextIndex - 1;
    long lastLogIndex = logStore.getLastLogIndex();

    long previousLogTerm =
        previousLogIndex == 0
            ? 0
            : logStore
                .getTerm(previousLogIndex)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Missing previous log term at index " + previousLogIndex));

    List<RaftLogEntry> entries =
        nextIndex > lastLogIndex ? List.of() : logStore.getEntries(nextIndex, lastLogIndex);

    long replicatedThroughIndex =
        entries.isEmpty() ? previousLogIndex : entries.get(entries.size() - 1).getIndex();

    AppendEntriesRequest request =
        new AppendEntriesRequest(
            term,
            localNodeId,
            previousLogIndex,
            previousLogTerm,
            entries,
            commitIndexTracker.getCommitIndex());

    Object requestToken = new Object();
    replicationRequestsInFlight.put(peerId, requestToken);

    try {
      peerClient
          .appendEntries(peerId, request)
          .whenComplete(
              (response, error) ->
                  handleResponse(
                      peerId,
                      term,
                      nextIndex,
                      replicatedThroughIndex,
                      requestToken,
                      response,
                      error));
    } catch (RuntimeException exception) {
      replicationRequestsInFlight.remove(peerId, requestToken);
      throw exception;
    }
  }

  private void sendInstallSnapshot(String peerId, long term) {
    if (snapshotStore.isEmpty() || replicationRequestsInFlight.containsKey(peerId)) {
      return;
    }

    RaftSnapshot snapshot = snapshotStore.get().load().orElse(null);

    if (snapshot == null) {
      return;
    }

    long snapshotIndex = snapshot.getLastIncludedIndex();
    long lastIncludedIndex = logStore.getLastIncludedIndex();

    // Never send a snapshot older than the leader's compacted log boundary.
    if (snapshotIndex < lastIncludedIndex || snapshotIndex > logStore.getLastLogIndex()) {
      return;
    }

    // Verify that the snapshot represents the same log position and term.
    Optional<Long> localSnapshotTerm = logStore.getTerm(snapshotIndex);

    if (localSnapshotTerm.isEmpty() || localSnapshotTerm.get() != snapshot.getLastIncludedTerm()) {
      return;
    }

    InstallSnapshotRequest request =
        new InstallSnapshotRequest(
            term, localNodeId, snapshotIndex, snapshot.getLastIncludedTerm(), snapshot.getData());

    // Track this specific transfer so stale responses cannot affect newer transfers.
    Object transferToken = new Object();
    replicationRequestsInFlight.put(peerId, transferToken);

    try {
      peerClient
          .installSnapshot(peerId, request)
          .whenComplete(
              (response, error) ->
                  handleInstallSnapshotResponse(
                      peerId, term, snapshotIndex, transferToken, response, error));
    } catch (RuntimeException exception) {
      replicationRequestsInFlight.remove(peerId, transferToken);
      throw exception;
    }
  }

  private synchronized void handleInstallSnapshotResponse(
      String peerId,
      long requestTerm,
      long snapshotIndex,
      Object transferToken,
      InstallSnapshotResponse response,
      Throwable error) {

    Object activeTransfer = replicationRequestsInFlight.get(peerId);

    // Remove only the transfer associated with this response.
    if (activeTransfer == transferToken) {
      replicationRequestsInFlight.remove(peerId);
    }

    // Always honor a higher term, even if this response belongs to an old transfer.
    if (response != null && response.getTerm() > nodeState.getCurrentTerm()) {
      nodeState.advanceTermIfHigher(response.getTerm());
      stopScheduling();
      return;
    }

    // Ignore stale responses and unsuccessful RPC completions.
    if (activeTransfer != transferToken
        || closed
        || !started
        || error != null
        || response == null) {
      return;
    }

    if (requestTerm != nodeState.getCurrentTerm()
        || nodeState.getRole() != RaftRole.LEADER
        || response.getTerm() != requestTerm) {
      return;
    }

    RaftPeerReplicationState replicationState = replicationStates.get(peerId);

    if (replicationState == null) {
      return;
    }

    // Never regress progress when an older asynchronous response arrives.
    if (!response.isSuccess()) {
      return;
    }
    replicationState.recordSuccess(snapshotIndex);

    advanceCommitIndex();
  }

  private synchronized void handleResponse(
      String peerId,
      long requestTerm,
      long attemptedNextIndex,
      long replicatedThroughIndex,
      Object requestToken,
      AppendEntriesResponse response,
      Throwable error) {

    Object activeRequest = replicationRequestsInFlight.get(peerId);

    if (activeRequest == requestToken) {
      replicationRequestsInFlight.remove(peerId);
    }

    if (activeRequest != requestToken) {
      return;
    }

    if (closed || !started || error != null || response == null) {
      return;
    }

    if (response.getTerm() > nodeState.getCurrentTerm()) {
      nodeState.advanceTermIfHigher(response.getTerm());
      stopScheduling();
      return;
    }

    if (requestTerm != nodeState.getCurrentTerm() || nodeState.getRole() != RaftRole.LEADER) {
      return;
    }

    RaftPeerReplicationState replicationState = replicationStates.get(peerId);

    if (replicationState == null) {
      return;
    }

    if (response.isSuccess()) {
      replicationState.recordSuccess(replicatedThroughIndex);
      advanceCommitIndex();
    } else {
      replicationState.recordFailure(attemptedNextIndex);
    }
  }

  private synchronized void advanceCommitIndex() {
    List<Long> replicatedIndexes = new ArrayList<>();

    // The leader always counts its own log as replicated.
    replicatedIndexes.add(logStore.getLastLogIndex());

    for (RaftPeerReplicationState state : replicationStates.values()) {
      replicatedIndexes.add(state.getMatchIndex());
    }

    commitIndexTracker.advanceCommitIndex(replicatedIndexes);

    // Apply any entries that have become committed.
    stateMachineApplier.ifPresent(RaftStateMachineApplier::applyCommittedEntries);
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
    replicationRequestsInFlight.clear();
  }

  /** Closes the manager without shutting down the caller-owned scheduler. */
  @Override
  public synchronized void close() {
    stop();
    closed = true;
  }
}
