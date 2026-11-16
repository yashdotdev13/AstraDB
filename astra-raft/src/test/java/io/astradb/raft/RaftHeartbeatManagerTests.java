package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.election.RaftHeartbeatManager;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.rpc.InstallSnapshotRequest;
import io.astradb.raft.rpc.InstallSnapshotResponse;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftHeartbeatManagerTests {

  private RaftNodeState nodeState;
  private InMemoryMetadataStore metadataStore;
  private InMemoryLogStore logStore;
  private FakePeerClient peerClient;
  private TestScheduler scheduler;
  private RaftHeartbeatManager heartbeatManager;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    logStore = new InMemoryLogStore();
    peerClient = new FakePeerClient();
    scheduler = new TestScheduler();

    becomeLeader();

    heartbeatManager =
        new RaftHeartbeatManager(
            "node-1", Set.of("node-2", "node-3"), nodeState, logStore, peerClient, scheduler, 100);
  }

  @Test
  void shouldRejectStartingWhenNodeIsNotLeader() {
    nodeState.becomeFollower();

    assertThrows(IllegalStateException.class, () -> heartbeatManager.start());
    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldSchedulePeriodicHeartbeats() {
    heartbeatManager.start();

    assertEquals(1, scheduler.getScheduledTaskCount());
    assertEquals(100, scheduler.getLastScheduledDelayMillis());
  }

  @Test
  void shouldMakeStartIdempotent() {
    heartbeatManager.start();
    heartbeatManager.start();

    assertEquals(1, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldSendHeartbeatsToEveryPeer() {
    heartbeatManager.start();

    scheduler.runNextTask();

    assertEquals(2, peerClient.requests.size());
    assertTrue(peerClient.requests.containsKey("node-2"));
    assertTrue(peerClient.requests.containsKey("node-3"));
  }

  @Test
  void shouldIncludeCurrentTermAndLastLogPositionInHeartbeat() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    AppendEntriesRequest request = peerClient.requests.get("node-2").request;

    assertEquals(1, request.getTerm());
    assertEquals("node-1", request.getLeaderId());
    assertEquals(3, request.getPreviousLogIndex());
    assertEquals(2, request.getPreviousLogTerm());
    assertTrue(request.getEntries().isEmpty());
    assertEquals(0, request.getLeaderCommit());
  }

  @Test
  void shouldScheduleNextHeartbeatAfterSendingHeartbeats() {
    heartbeatManager.start();

    scheduler.runNextTask();

    assertEquals(2, peerClient.requests.size());
    assertEquals(1, scheduler.getScheduledTaskCount());
    assertEquals(100, scheduler.getLastScheduledDelayMillis());
  }

  @Test
  void shouldStepDownWhenPeerReportsHigherTerm() {
    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.respond("node-2", 2, false);

    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldContinueHeartbeatsWhenPeerRequestFails() {
    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.fail("node-2");

    assertEquals(RaftRole.LEADER, nodeState.getRole());
    assertEquals(1, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldStopSchedulingWhenNodeLosesLeadership() {
    heartbeatManager.start();

    nodeState.becomeFollower();
    scheduler.runNextTask();

    assertEquals(0, scheduler.getScheduledTaskCount());
    assertTrue(peerClient.requests.isEmpty());
  }

  @Test
  void shouldCancelPendingHeartbeatWhenStopped() {
    heartbeatManager.start();

    assertEquals(1, scheduler.getScheduledTaskCount());

    heartbeatManager.stop();

    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldNotSendHeartbeatsAfterStopping() {
    heartbeatManager.start();
    heartbeatManager.stop();

    heartbeatManager.sendHeartbeats();

    assertTrue(peerClient.requests.isEmpty());
    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldRejectStartingAfterClosing() {
    heartbeatManager.start();
    heartbeatManager.close();

    assertThrows(IllegalStateException.class, () -> heartbeatManager.start());
    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldNotShutdownCallerOwnedSchedulerWhenClosed() {
    heartbeatManager.start();

    heartbeatManager.close();

    assertFalse(scheduler.isShutdown());
  }

  @Test
  void shouldRejectInvalidHeartbeatInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RaftHeartbeatManager(
                "node-1", Set.of("node-2"), nodeState, logStore, peerClient, scheduler, 0));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RaftHeartbeatManager(
                "node-1", Set.of("node-2"), nodeState, logStore, peerClient, scheduler, -1));
  }

  @Test
  void shouldRejectInvalidPeerConfiguration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RaftHeartbeatManager(
                "node-1", Set.of("node-1"), nodeState, logStore, peerClient, scheduler, 100));
  }

  @Test
  void shouldRejectBlankLocalNodeId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RaftHeartbeatManager(
                " ", Set.of("node-2"), nodeState, logStore, peerClient, scheduler, 100));
  }

  @Test
  void shouldInitiallySendHeartbeatAtEndOfLeaderLog() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    AppendEntriesRequest request = peerClient.requests.get("node-2").request;

    assertEquals(3, request.getPreviousLogIndex());
    assertTrue(request.getEntries().isEmpty());
  }

  @Test
  void shouldSendMissingEntriesAfterFollowerRejectsPreviousLog() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, false);

    scheduler.runNextTask();

    AppendEntriesRequest retry = peerClient.requests.get("node-2").request;

    assertEquals(2, retry.getPreviousLogIndex());
    assertEquals(1, retry.getPreviousLogTerm());
    assertEquals(1, retry.getEntries().size());
    assertEquals(3, retry.getEntries().get(0).getIndex());
    assertEquals(2, retry.getEntries().get(0).getTerm());
  }

  @Test
  void shouldRetryFromBeginningWhenFollowerHasEmptyLog() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, false);
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, false);
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, false);
    scheduler.runNextTask();

    AppendEntriesRequest retry = peerClient.requests.get("node-2").request;

    assertEquals(0, retry.getPreviousLogIndex());
    assertEquals(0, retry.getPreviousLogTerm());
    assertEquals(3, retry.getEntries().size());
    assertEquals(1, retry.getEntries().get(0).getIndex());
    assertEquals(2, retry.getEntries().get(1).getIndex());
    assertEquals(3, retry.getEntries().get(2).getIndex());
  }

  @Test
  void shouldAdvanceReplicationProgressAfterSuccessfulAppendEntries() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, false);
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, true);
    scheduler.runNextTask();

    AppendEntriesRequest nextRequest = peerClient.requests.get("node-2").request;

    assertEquals(3, nextRequest.getPreviousLogIndex());
    assertTrue(nextRequest.getEntries().isEmpty());
  }

  @Test
  void shouldMaintainIndependentReplicationProgressForEachFollower() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.respond("node-2", 1, false);
    peerClient.respond("node-3", 1, true);

    scheduler.runNextTask();

    AppendEntriesRequest node2Request = peerClient.requests.get("node-2").request;
    AppendEntriesRequest node3Request = peerClient.requests.get("node-3").request;

    assertEquals(2, node2Request.getPreviousLogIndex());
    assertEquals(1, node2Request.getEntries().size());
    assertEquals(3, node2Request.getEntries().get(0).getIndex());

    assertEquals(3, node3Request.getPreviousLogIndex());
    assertTrue(node3Request.getEntries().isEmpty());
  }

  @Test
  void shouldNotAdvanceReplicationProgressWhenPeerRequestFails() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));

    heartbeatManager.start();
    scheduler.runNextTask();

    peerClient.fail("node-2");
    scheduler.runNextTask();

    AppendEntriesRequest retry = peerClient.requests.get("node-2").request;

    assertEquals(2, retry.getPreviousLogIndex());
    assertTrue(retry.getEntries().isEmpty());
  }

  @Test
  void shouldNotPropagateCommitIndexForEntriesFromOlderTerms() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 1, "three")));

    heartbeatManager.start();
    scheduler.runNextTask();

    // The leader is now in term 2, but these entries belong to term 1.
    peerClient.respond("node-2", 2, true);

    scheduler.runNextTask();

    AppendEntriesRequest request = peerClient.requests.get("node-2").request;

    assertEquals(0, request.getLeaderCommit());
  }

  private void becomeLeader() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();
  }

  private RaftLogEntry entry(long index, long term, String payload) {
    return new RaftLogEntry(index, term, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static final class FakePeerClient implements RaftPeerClient {

    private final Map<String, PendingRequest> requests = new HashMap<>();
    private final Map<String, PendingSnapshotRequest> snapshotRequests = new HashMap<>();

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(
        String peerId, RequestVoteRequest request) {
      return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(
        String peerId, AppendEntriesRequest request) {

      CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();

      requests.put(peerId, new PendingRequest(request, future));

      return future;
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(
        String peerId, InstallSnapshotRequest request) {

      CompletableFuture<InstallSnapshotResponse> future = new CompletableFuture<>();

      snapshotRequests.put(peerId, new PendingSnapshotRequest(request, future));

      return future;
    }

    void respondToSnapshot(String peerId, long term) {
      snapshotRequests.get(peerId).future.complete(new InstallSnapshotResponse(term, true));
    }

    void failSnapshot(String peerId) {
      snapshotRequests
          .get(peerId)
          .future
          .completeExceptionally(new RuntimeException("Snapshot transfer failed"));
    }

    void respond(String peerId, long term, boolean success) {
      requests.get(peerId).future.complete(new AppendEntriesResponse(term, success));
    }

    void fail(String peerId) {
      requests.get(peerId).future.completeExceptionally(new RuntimeException("Peer unavailable"));
    }
  }

  private static final class PendingRequest {

    private final AppendEntriesRequest request;
    private final CompletableFuture<AppendEntriesResponse> future;

    private PendingRequest(
        AppendEntriesRequest request, CompletableFuture<AppendEntriesResponse> future) {
      this.request = request;
      this.future = future;
    }
  }

  private static final class PendingSnapshotRequest {

    private final InstallSnapshotRequest request;
    private final CompletableFuture<InstallSnapshotResponse> future;

    private PendingSnapshotRequest(
        InstallSnapshotRequest request, CompletableFuture<InstallSnapshotResponse> future) {
      this.request = request;
      this.future = future;
    }
  }

  private static final class InMemoryMetadataStore implements RaftMetadataStore {

    private RaftMetadata metadata = new RaftMetadata(0, null);

    @Override
    public RaftMetadata loadMetadata() {
      return metadata;
    }

    @Override
    public void saveMetadata(RaftMetadata metadata) {
      this.metadata = metadata;
    }
  }

  private static final class InMemoryLogStore implements RaftLogStore {

    private final List<RaftLogEntry> entries = new ArrayList<>();
    private long lastIncludedIndex;
    private long lastIncludedTerm;

    @Override
    public synchronized void append(RaftLogEntry entry) {
      appendAll(List.of(entry));
    }

    @Override
    public synchronized void appendAll(List<RaftLogEntry> newEntries) {
      for (RaftLogEntry entry : newEntries) {
        if (entry.getIndex() != getLastLogIndex() + 1) {
          throw new IllegalArgumentException("Non-sequential log entry");
        }

        entries.add(entry);
      }
    }

    @Override
    public synchronized Optional<RaftLogEntry> getEntry(long index) {
      if (index <= lastIncludedIndex || index > getLastLogIndex()) {
        return Optional.empty();
      }

      return Optional.of(entries.get(Math.toIntExact(index - lastIncludedIndex - 1)));
    }

    @Override
    public synchronized List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      if (fromIndex <= lastIncludedIndex || toIndex < fromIndex || toIndex > getLastLogIndex()) {
        throw new IllegalArgumentException("Invalid log range");
      }

      int fromOffset = Math.toIntExact(fromIndex - lastIncludedIndex - 1);
      int toOffset = Math.toIntExact(toIndex - lastIncludedIndex);

      return List.copyOf(entries.subList(fromOffset, toOffset));
    }

    @Override
    public synchronized long getLastLogIndex() {
      return entries.isEmpty() ? lastIncludedIndex : entries.get(entries.size() - 1).getIndex();
    }

    @Override
    public synchronized long getLastLogTerm() {
      return entries.isEmpty() ? lastIncludedTerm : entries.get(entries.size() - 1).getTerm();
    }

    @Override
    public synchronized long getLastIncludedIndex() {
      return lastIncludedIndex;
    }

    @Override
    public synchronized long getLastIncludedTerm() {
      return lastIncludedTerm;
    }

    @Override
    public synchronized void compactPrefix(long index, long term) {
      if (index <= lastIncludedIndex || index > getLastLogIndex()) {
        throw new IllegalArgumentException("Invalid snapshot boundary");
      }

      RaftLogEntry boundaryEntry =
          getEntry(index)
              .orElseThrow(() -> new IllegalArgumentException("Missing snapshot boundary entry"));

      if (boundaryEntry.getTerm() != term) {
        throw new IllegalArgumentException("Snapshot term does not match log");
      }

      entries.removeIf(entry -> entry.getIndex() <= index);
      lastIncludedIndex = index;
      lastIncludedTerm = term;
    }

    @Override
    public synchronized void truncateSuffix(long fromIndex) {
      replaceSuffix(fromIndex, List.of());
    }

    @Override
    public synchronized void flush() {}

    @Override
    public synchronized void replaceSuffix(long fromIndex, List<RaftLogEntry> replacements) {
      if (fromIndex <= lastIncludedIndex || fromIndex > getLastLogIndex() + 1) {
        throw new IllegalArgumentException("Invalid replacement index");
      }

      entries.removeIf(entry -> entry.getIndex() >= fromIndex);
      appendAll(replacements);
    }

    @Override
    public synchronized void close() {}

    public synchronized void installSnapshotBoundary(long index, long term) {
      if (index <= lastIncludedIndex || term <= 0 || index > getLastLogIndex()) {
        throw new IllegalArgumentException("Invalid snapshot boundary");
      }

      entries.removeIf(entry -> entry.getIndex() <= index);
      lastIncludedIndex = index;
      lastIncludedTerm = term;
    }
  }

  /** Deterministic scheduler for heartbeat lifecycle tests. */
  private static final class TestScheduler extends AbstractExecutorService
      implements ScheduledExecutorService {

    private final List<TestScheduledFuture> tasks = new ArrayList<>();
    private boolean shutdown;

    @Override
    public synchronized ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {

      if (shutdown) {
        throw new IllegalStateException("Scheduler is shut down");
      }

      TestScheduledFuture future = new TestScheduledFuture(command, unit.toMillis(delay));

      tasks.add(future);

      return future;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException("Callable scheduling is not required");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      throw new UnsupportedOperationException("Periodic scheduling is not required");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException("Periodic scheduling is not required");
    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }

    synchronized int getScheduledTaskCount() {
      return (int) tasks.stream().filter(task -> !task.isCancelled() && !task.isDone()).count();
    }

    synchronized long getLastScheduledDelayMillis() {
      return tasks.get(tasks.size() - 1).delayMillis;
    }

    synchronized void runNextTask() {
      TestScheduledFuture task =
          tasks.stream()
              .filter(candidate -> !candidate.isCancelled() && !candidate.isDone())
              .findFirst()
              .orElseThrow(() -> new AssertionError("No pending scheduled task"));

      task.run();
    }

    @Override
    public synchronized void shutdown() {
      shutdown = true;
    }

    @Override
    public synchronized List<Runnable> shutdownNow() {
      shutdown = true;
      return List.of();
    }

    @Override
    public synchronized boolean isShutdown() {
      return shutdown;
    }

    @Override
    public synchronized boolean isTerminated() {
      return shutdown;
    }

    @Override
    public synchronized boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }
  }

  private static final class TestScheduledFuture implements ScheduledFuture<Object> {

    private final Runnable command;
    private final long delayMillis;
    private boolean cancelled;
    private boolean done;

    private TestScheduledFuture(Runnable command, long delayMillis) {
      this.command = command;
      this.delayMillis = delayMillis;
    }

    void run() {
      if (!cancelled && !done) {
        done = true;
        command.run();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (done) {
        return false;
      }

      cancelled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return done || cancelled;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
      throw new UnsupportedOperationException("Future result is not used");
    }

    @Override
    public Object get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException("Future result is not used");
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
  }
}
