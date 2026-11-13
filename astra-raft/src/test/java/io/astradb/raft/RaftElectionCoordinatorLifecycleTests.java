package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.election.ElectionTimeout;
import io.astradb.raft.election.RaftElectionConfig;
import io.astradb.raft.election.RaftElectionCoordinator;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftElectionCoordinatorLifecycleTests {

  private InMemoryMetadataStore metadataStore;
  private RaftNodeState nodeState;
  private InMemoryLogStore logStore;
  private FakePeerClient peerClient;
  private TestScheduler scheduler;
  private RaftElectionCoordinator coordinator;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    logStore = new InMemoryLogStore();
    peerClient = new FakePeerClient();
    scheduler = new TestScheduler();

    coordinator =
        new RaftElectionCoordinator(
            "node-1",
            Set.of("node-2", "node-3", "node-4", "node-5"),
            nodeState,
            logStore,
            peerClient,
            new ElectionTimeout(new RaftElectionConfig(150, 300), new FixedRandomGenerator()),
            scheduler);
  }

  @Test
  void shouldScheduleElectionTimeoutWhenStarted() {
    coordinator.start();

    assertEquals(1, scheduler.getScheduledTaskCount());
    assertEquals(150, scheduler.getLastScheduledDelayMillis());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(peerClient.requests.isEmpty());
  }

  @Test
  void shouldStartElectionWhenTimeoutExpires() {
    coordinator.start();

    scheduler.runNextTask();

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
    assertEquals(4, peerClient.requests.size());
  }

  @Test
  void shouldScheduleAnotherTimeoutAfterElectionStarts() {
    coordinator.start();

    scheduler.runNextTask();

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(1, scheduler.getScheduledTaskCount());

    scheduler.runNextTask();

    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(4, peerClient.requests.size());
  }

  @Test
  void shouldCancelElectionTimeoutAfterBecomingLeader() {
    coordinator.start();

    scheduler.runNextTask();

    peerClient.respond("node-2", 1, true);
    peerClient.respond("node-3", 1, true);

    assertEquals(RaftRole.LEADER, nodeState.getRole());
    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldCancelTimeoutWhenStopped() {
    coordinator.start();

    assertEquals(1, scheduler.getScheduledTaskCount());

    coordinator.stop();

    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldIgnoreResponsesAfterCoordinatorIsStopped() {
    coordinator.start();
    scheduler.runNextTask();

    assertEquals(1, nodeState.getCurrentTerm());

    coordinator.stop();

    peerClient.respond("node-2", 2, false);

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
  }

  @Test
  void shouldNotScheduleTimeoutAfterCoordinatorIsStopped() {
    coordinator.start();
    coordinator.stop();

    assertEquals(0, scheduler.getScheduledTaskCount());

    coordinator.startElection();

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldRejectStartingAfterCoordinatorIsClosed() {
    coordinator.start();
    coordinator.close();

    assertThrows(IllegalStateException.class, () -> coordinator.start());
    assertThrows(IllegalStateException.class, () -> coordinator.startElection());

    assertEquals(0, scheduler.getScheduledTaskCount());
  }

  @Test
  void shouldNotShutdownCallerOwnedSchedulerWhenClosed() {
    coordinator.start();

    coordinator.close();

    assertFalse(scheduler.isShutdown());
  }

  @Test
  void shouldMakeStartIdempotent() {
    coordinator.start();
    coordinator.start();

    assertEquals(1, scheduler.getScheduledTaskCount());
  }

  private static final class FixedRandomGenerator implements java.util.random.RandomGenerator {

    @Override
    public long nextLong(long origin, long bound) {
      return origin;
    }

    @Override
    public long nextLong() {
      return 0;
    }
  }

  private static final class FakePeerClient implements RaftPeerClient {

    private final Map<String, CompletableFuture<RequestVoteResponse>> requests = new HashMap<>();

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(
        String peerId, RequestVoteRequest request) {
      CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
      requests.put(peerId, future);
      return future;
    }

    void respond(String peerId, long term, boolean granted) {
      requests.get(peerId).complete(new RequestVoteResponse(term, granted));
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

    @Override
    public void append(RaftLogEntry entry) {}

    @Override
    public void appendAll(List<RaftLogEntry> entries) {}

    @Override
    public Optional<RaftLogEntry> getEntry(long index) {
      return Optional.empty();
    }

    @Override
    public List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      return List.of();
    }

    @Override
    public long getLastLogIndex() {
      return 0;
    }

    @Override
    public long getLastLogTerm() {
      return 0;
    }

    @Override
    public void truncateSuffix(long fromIndex) {}

    @Override
    public void flush() {}

    @Override
    public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {}

    @Override
    public void close() {}
  }

  /** Scheduler that allows tests to trigger scheduled tasks without sleeping. */
  private static final class TestScheduler extends AbstractExecutorService
      implements ScheduledExecutorService {

    private final List<TestScheduledFuture> tasks = new ArrayList<>();
    private final AtomicLong sequence = new AtomicLong();
    private boolean shutdown;

    @Override
    public synchronized ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      if (shutdown) {
        throw new IllegalStateException("Scheduler is shut down");
      }

      TestScheduledFuture future =
          new TestScheduledFuture(command, unit.toMillis(delay), sequence.getAndIncrement());

      tasks.add(future);
      return future;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(
        java.util.concurrent.Callable<V> callable, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException("Callable scheduling is not required by these tests");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      throw new UnsupportedOperationException("Periodic scheduling is not required by these tests");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException("Periodic scheduling is not required by these tests");
    }

    @Override
    public synchronized void execute(Runnable command) {
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
    private final long sequence;
    private boolean cancelled;
    private boolean done;

    private TestScheduledFuture(Runnable command, long delayMillis, long sequence) {
      this.command = command;
      this.delayMillis = delayMillis;
      this.sequence = sequence;
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
      throw new UnsupportedOperationException("Future result is not used by these tests");
    }

    @Override
    public Object get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException("Future result is not used by these tests");
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      if (other == this) {
        return 0;
      }

      if (other instanceof TestScheduledFuture future) {
        return Long.compare(sequence, future.sequence);
      }

      return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
  }
}
