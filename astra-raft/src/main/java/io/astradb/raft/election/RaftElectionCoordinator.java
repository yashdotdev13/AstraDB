package io.astradb.raft.election;

import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Coordinates Raft leader elections and collects votes from peers. */
public final class RaftElectionCoordinator implements AutoCloseable {

  private final String localNodeId;
  private final Set<String> peerIds;
  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;
  private final RaftPeerClient peerClient;
  private final ElectionTimeout electionTimeout;
  private final ScheduledExecutorService scheduler;

  private ElectionContext activeElection;
  private ScheduledFuture<?> electionTimeoutTask;

  private boolean started;
  private boolean closed;

  /** Creates a coordinator without automatic election scheduling. */
  public RaftElectionCoordinator(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient) {

    this(localNodeId, peerIds, nodeState, logStore, peerClient, null, null);
  }

  /** Creates a coordinator with automatic election timeout scheduling. */
  public RaftElectionCoordinator(
      String localNodeId,
      Set<String> peerIds,
      RaftNodeState nodeState,
      RaftLogStore logStore,
      RaftPeerClient peerClient,
      ElectionTimeout electionTimeout,
      ScheduledExecutorService scheduler) {

    this.localNodeId = Objects.requireNonNull(localNodeId, "Local node ID must not be null");

    if (localNodeId.isBlank()) {
      throw new IllegalArgumentException("Local node ID must not be blank");
    }

    Objects.requireNonNull(peerIds, "Peer IDs must not be null");

    this.peerIds = Set.copyOf(new HashSet<>(peerIds));

    if (this.peerIds.contains(localNodeId)) {
      throw new IllegalArgumentException("Peer IDs must not contain the local node");
    }

    if (this.peerIds.stream().anyMatch(String::isBlank)) {
      throw new IllegalArgumentException("Peer IDs must not contain blank values");
    }

    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Raft log store must not be null");
    this.peerClient = Objects.requireNonNull(peerClient, "Peer client must not be null");

    if ((electionTimeout == null) != (scheduler == null)) {
      throw new IllegalArgumentException(
          "Election timeout and scheduler must either both be provided or both be null");
    }

    this.electionTimeout = electionTimeout;
    this.scheduler = scheduler;
  }

  /**
   * Starts the automatic election timeout lifecycle.
   *
   * <p>The coordinator schedules randomized election timeouts and starts elections when they
   * expire.
   */
  public synchronized void start() {
    if (closed) {
      throw new IllegalStateException("Election coordinator is closed");
    }

    if (started) {
      return;
    }

    if (electionTimeout == null || scheduler == null) {
      throw new IllegalStateException("Election timeout and scheduler must be configured");
    }

    started = true;
    scheduleElectionTimeout();
  }

  /** Starts a new election and asynchronously requests votes from all peers. */
  public synchronized long startElection() {
    if (closed) {
      throw new IllegalStateException("Election coordinator is closed");
    }

    if (nodeState.getRole() == RaftRole.LEADER) {
      throw new IllegalStateException("A leader cannot start an election");
    }

    cancelElectionTimeout();

    nodeState.startElection(localNodeId);

    long electionTerm = nodeState.getCurrentTerm();

    VoteCounter voteCounter = new VoteCounter(peerIds.size() + 1, localNodeId);

    ElectionContext context = new ElectionContext(electionTerm, voteCounter);

    activeElection = context;

    if (voteCounter.hasMajority()) {
      if (nodeState.becomeLeader(electionTerm)) {
        activeElection = null;
        cancelElectionTimeout();
      }
      return electionTerm;
    }

    // Schedule another election attempt if this election does not succeed in time.
    if (started) {
      scheduleElectionTimeout();
    }

    RequestVoteRequest request =
        new RequestVoteRequest(
            electionTerm, localNodeId, logStore.getLastLogIndex(), logStore.getLastLogTerm());

    for (String peerId : peerIds) {
      try {
        peerClient
            .requestVote(peerId, request)
            .whenComplete(
                (response, error) -> handleVoteResponse(context, peerId, response, error));
      } catch (RuntimeException exception) {
        handleVoteResponse(context, peerId, null, exception);
      }
    }

    return electionTerm;
  }

  /** Handles an asynchronous response from a peer. */
  private synchronized void handleVoteResponse(
      ElectionContext context, String peerId, RequestVoteResponse response, Throwable error) {

    if (closed || activeElection != context) {
      return;
    }

    if (!peerIds.contains(peerId)) {
      return;
    }

    if (error != null || response == null) {
      recordRejectedVote(context, peerId);
      return;
    }

    // A higher-term response requires an immediate step-down.
    if (response.getTerm() > nodeState.getCurrentTerm()) {
      nodeState.advanceTermIfHigher(response.getTerm());
      finishElectionAndReschedule(context);
      return;
    }

    // Ignore responses if this election is no longer current.
    if (nodeState.getCurrentTerm() != context.term || nodeState.getRole() != RaftRole.CANDIDATE) {
      return;
    }

    // A response from an older term cannot grant a vote.
    if (response.getTerm() < context.term) {
      recordRejectedVote(context, peerId);
      return;
    }

    context.voteCounter.recordVote(peerId, response.isVoteGranted());

    if (context.voteCounter.hasMajority()) {
      if (nodeState.becomeLeader(context.term)) {
        activeElection = null;
        cancelElectionTimeout();
      }
    } else if (context.voteCounter.isMajorityImpossible()) {
      finishElectionAndReschedule(context);
    }
  }

  /** Records a failed or stale peer response. */
  private synchronized void recordRejectedVote(ElectionContext context, String peerId) {
    if (activeElection != context) {
      return;
    }

    context.voteCounter.recordVote(peerId, false);

    if (context.voteCounter.isMajorityImpossible()) {
      finishElectionAndReschedule(context);
    }
  }

  /** Finishes an unsuccessful election and schedules another attempt when enabled. */
  private synchronized void finishElectionAndReschedule(ElectionContext context) {
    if (activeElection != context) {
      return;
    }

    activeElection = null;

    if (started && !closed && nodeState.getRole() != RaftRole.LEADER) {
      scheduleElectionTimeout();
    }
  }

  /** Schedules the next randomized election timeout. */
  private synchronized void scheduleElectionTimeout() {
    if (!started || closed || nodeState.getRole() == RaftRole.LEADER) {
      return;
    }

    cancelElectionTimeout();

    long timeoutMillis = electionTimeout.nextTimeoutMillis();

    electionTimeoutTask =
        scheduler.schedule(this::onElectionTimeout, timeoutMillis, TimeUnit.MILLISECONDS);
  }

  /** Handles expiration of the election timeout. */
  private synchronized void onElectionTimeout() {
    if (!started || closed || nodeState.getRole() == RaftRole.LEADER) {
      return;
    }

    electionTimeoutTask = null;

    try {
      startElection();
    } catch (RuntimeException exception) {
      if (started && !closed) {
        scheduleElectionTimeout();
      }
    }
  }

  /** Cancels the currently scheduled election timeout. */
  private synchronized void cancelElectionTimeout() {
    if (electionTimeoutTask != null) {
      electionTimeoutTask.cancel(false);
      electionTimeoutTask = null;
    }
  }

  /** Stops automatic election scheduling and invalidates the active election. */
  public synchronized void stop() {
    started = false;
    activeElection = null;
    cancelElectionTimeout();
  }

  /**
   * Closes the coordinator.
   *
   * <p>The injected scheduler remains owned by the caller and is not shut down here.
   */
  @Override
  public synchronized void close() {
    stop();
    closed = true;
  }

  private static final class ElectionContext {

    private final long term;
    private final VoteCounter voteCounter;

    private ElectionContext(long term, VoteCounter voteCounter) {
      this.term = term;
      this.voteCounter = voteCounter;
    }
  }
}
