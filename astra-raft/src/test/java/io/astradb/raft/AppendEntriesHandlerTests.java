package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.AppendEntriesHandler;
import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AppendEntriesHandlerTests {

  private InMemoryMetadataStore metadataStore;
  private RaftNodeState nodeState;
  private InMemoryLogStore logStore;
  private AtomicInteger timeoutResetCount;
  private AppendEntriesHandler handler;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    logStore = new InMemoryLogStore();
    timeoutResetCount = new AtomicInteger();

    handler = new AppendEntriesHandler(nodeState, logStore, timeoutResetCount::incrementAndGet);
  }

  @Test
  void shouldRejectRequestFromStaleTerm() {
    nodeState.advanceTerm(2);

    AppendEntriesResponse response = handler.handle(request(1, 0, 0, List.of()));

    assertFalse(response.isSuccess());
    assertEquals(2, response.getTerm());
    assertEquals(0, timeoutResetCount.get());
  }

  @Test
  void shouldAdvanceTermWhenRequestHasHigherTerm() {
    AppendEntriesResponse response = handler.handle(request(3, 0, 0, List.of()));

    assertTrue(response.isSuccess());
    assertEquals(3, response.getTerm());
    assertEquals(3, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldPersistHigherTermAndClearPreviousVote() {
    nodeState.startElection("node-1");

    handler.handle(request(2, 0, 0, List.of()));

    assertEquals(2, metadataStore.metadata.getCurrentTerm());
    assertTrue(metadataStore.metadata.getVotedFor().isEmpty());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldBecomeFollowerWhenReceivingSameTermLeaderRequest() {
    nodeState.startElection("node-1");

    AppendEntriesResponse response = handler.handle(request(1, 0, 0, List.of()));

    assertTrue(response.isSuccess());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(1, timeoutResetCount.get());
  }

  @Test
  void shouldAcceptHeartbeatWithEmptyLogEntries() {
    AppendEntriesResponse response = handler.handle(request(1, 0, 0, List.of()));

    assertTrue(response.isSuccess());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, logStore.getLastLogIndex());
  }

  @Test
  void shouldResetTimeoutWhenPreviousLogEntryDoesNotMatch() {
    logStore.append(entry(1, 1, "local"));

    AppendEntriesResponse response = handler.handle(request(2, 1, 2, List.of()));

    assertFalse(response.isSuccess());
    assertEquals(2, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(1, logStore.getLastLogIndex());
  }

  @Test
  void shouldRejectRequestWhenPreviousLogIndexIsBeyondLocalLog() {
    AppendEntriesResponse response = handler.handle(request(1, 2, 1, List.of()));

    assertFalse(response.isSuccess());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, logStore.getLastLogIndex());
  }

  @Test
  void shouldAppendEntriesWhenLocalLogIsEmpty() {
    List<RaftLogEntry> entries =
        List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three"));

    AppendEntriesResponse response = handler.handle(request(2, 0, 0, entries));

    assertTrue(response.isSuccess());
    assertEquals(3, logStore.getLastLogIndex());
    assertEquals(2, logStore.getLastLogTerm());
    assertEquals("two", payloadAt(2));
  }

  @Test
  void shouldPreserveMatchingEntriesAndAppendMissingEntries() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));

    List<RaftLogEntry> entries = List.of(entry(2, 1, "two"), entry(3, 2, "three"));

    AppendEntriesResponse response = handler.handle(request(2, 1, 1, entries));

    assertTrue(response.isSuccess());
    assertEquals(3, logStore.getLastLogIndex());
    assertEquals("one", payloadAt(1));
    assertEquals("two", payloadAt(2));
    assertEquals("three", payloadAt(3));
  }

  @Test
  void shouldReplaceConflictingSuffix() {
    logStore.appendAll(
        List.of(entry(1, 1, "one"), entry(2, 1, "old-two"), entry(3, 1, "old-three")));

    List<RaftLogEntry> entries =
        List.of(entry(2, 2, "new-two"), entry(3, 2, "new-three"), entry(4, 2, "new-four"));

    AppendEntriesResponse response = handler.handle(request(2, 1, 1, entries));

    assertTrue(response.isSuccess());
    assertEquals(4, logStore.getLastLogIndex());
    assertEquals("one", payloadAt(1));
    assertEquals("new-two", payloadAt(2));
    assertEquals("new-three", payloadAt(3));
    assertEquals("new-four", payloadAt(4));
  }

  @Test
  void shouldAppendOnlyEntriesAfterMatchingPrefix() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));

    List<RaftLogEntry> entries =
        List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three"));

    AppendEntriesResponse response = handler.handle(request(2, 0, 0, entries));

    assertTrue(response.isSuccess());
    assertEquals(3, logStore.getLastLogIndex());
    assertEquals("three", payloadAt(3));
  }

  @Test
  void shouldNotRewriteLogWhenAllEntriesAlreadyMatch() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));

    AppendEntriesResponse response =
        handler.handle(request(2, 0, 0, List.of(entry(1, 1, "one"), entry(2, 1, "two"))));

    assertTrue(response.isSuccess());
    assertEquals(2, logStore.getLastLogIndex());
    assertEquals("two", payloadAt(2));
  }

  @Test
  void shouldAdvanceCommitIndexFromLeaderCommit() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));

    AppendEntriesResponse response = handler.handle(request(1, 0, 0, List.of(), 2));

    assertTrue(response.isSuccess());
    assertEquals(2, handler.getCommitIndex());
  }

  @Test
  void shouldNotAdvanceCommitIndexBeyondLocalLog() {
    logStore.append(entry(1, 1, "one"));

    AppendEntriesResponse response = handler.handle(request(1, 0, 0, List.of(), 5));

    assertTrue(response.isSuccess());
    assertEquals(1, handler.getCommitIndex());
  }

  @Test
  void shouldNeverMoveCommitIndexBackwards() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));

    handler.handle(request(1, 0, 0, List.of(), 2));
    handler.handle(request(1, 0, 0, List.of(), 1));

    assertEquals(2, handler.getCommitIndex());
  }

  @Test
  void shouldRejectConflictingEntryAtCommittedIndex() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "old-two")));

    handler.handle(request(1, 0, 0, List.of(), 2));

    AppendEntriesResponse response =
        handler.handle(request(1, 1, 1, List.of(entry(2, 2, "new-two")), 2));

    assertFalse(response.isSuccess());
    assertEquals(2, handler.getCommitIndex());
    assertEquals("old-two", payloadAt(2));
  }

  @Test
  void shouldRejectIncomingEntryAtSnapshotBoundary() {
    prepareCompactedLog();

    AppendEntriesResponse response =
        handler.handle(request(2, 4, 1, List.of(entry(5, 2, "duplicate-five"))));

    assertFalse(response.isSuccess());
    assertEquals(5, logStore.getLastLogIndex());
    assertEquals(5, logStore.getLastIncludedIndex());
  }

  @Test
  void shouldRejectIncomingEntryBeforeSnapshotBoundary() {
    prepareCompactedLog();

    AppendEntriesResponse response =
        handler.handle(request(2, 3, 1, List.of(entry(4, 1, "old-four"))));

    assertFalse(response.isSuccess());
    assertEquals(5, logStore.getLastLogIndex());
    assertEquals(5, logStore.getLastIncludedIndex());
  }

  @Test
  void shouldAppendEntryImmediatelyAfterSnapshotBoundary() {
    prepareCompactedLog();

    AppendEntriesResponse response = handler.handle(request(2, 5, 2, List.of(entry(6, 3, "six"))));

    assertTrue(response.isSuccess());
    assertEquals(6, logStore.getLastLogIndex());
    assertEquals(5, logStore.getLastIncludedIndex());
    assertEquals("six", payloadAt(6));
  }

  @Test
  void shouldRejectRequestContainingEntryAtPreviousLogIndex() {
    assertThrows(
        IllegalArgumentException.class,
        () -> request(2, 5, 2, List.of(entry(5, 2, "duplicate-five"))));
  }

  private void prepareCompactedLog() {
    logStore.appendAll(
        List.of(
            entry(1, 1, "one"),
            entry(2, 1, "two"),
            entry(3, 1, "three"),
            entry(4, 1, "four"),
            entry(5, 2, "five")));

    logStore.compactPrefix(5, 2);
  }

  private AppendEntriesRequest request(
      long term, long previousIndex, long previousTerm, List<RaftLogEntry> entries) {
    return request(term, previousIndex, previousTerm, entries, 0);
  }

  private AppendEntriesRequest request(
      long term,
      long previousIndex,
      long previousTerm,
      List<RaftLogEntry> entries,
      long leaderCommit) {

    return new AppendEntriesRequest(
        term, "leader-1", previousIndex, previousTerm, entries, leaderCommit);
  }

  private RaftLogEntry entry(long index, long term, String payload) {
    return new RaftLogEntry(index, term, payload.getBytes(StandardCharsets.UTF_8));
  }

  private String payloadAt(long index) {
    return new String(logStore.getEntry(index).orElseThrow().getPayload(), StandardCharsets.UTF_8);
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
        long expectedIndex = lastIncludedIndex + entries.size() + 1L;

        if (entry.getIndex() != expectedIndex) {
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

      return Optional.of(entries.get((int) (index - lastIncludedIndex - 1)));
    }

    @Override
    public synchronized List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      if (fromIndex <= lastIncludedIndex || toIndex < fromIndex || toIndex > getLastLogIndex()) {
        throw new IllegalArgumentException("Invalid log range");
      }

      int start = (int) (fromIndex - lastIncludedIndex - 1);
      int end = (int) (toIndex - lastIncludedIndex);

      return List.copyOf(entries.subList(start, end));
    }

    @Override
    public synchronized long getLastLogIndex() {
      return lastIncludedIndex + entries.size();
    }

    @Override
    public synchronized long getLastLogTerm() {
      if (entries.isEmpty()) {
        return lastIncludedTerm;
      }

      return entries.get(entries.size() - 1).getTerm();
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
    public synchronized Optional<Long> getTerm(long index) {
      if (index == lastIncludedIndex && index > 0) {
        return Optional.of(lastIncludedTerm);
      }

      if (index <= lastIncludedIndex) {
        return Optional.empty();
      }

      return getEntry(index).map(RaftLogEntry::getTerm);
    }

    @Override
    public synchronized void compactPrefix(long index, long term) {
      if (index <= lastIncludedIndex || index > getLastLogIndex()) {
        throw new IllegalArgumentException("Invalid snapshot boundary");
      }

      RaftLogEntry boundaryEntry = getEntry(index).orElseThrow();

      if (boundaryEntry.getTerm() != term) {
        throw new IllegalArgumentException("Snapshot boundary term mismatch");
      }

      int entriesToRemove = (int) (index - lastIncludedIndex);

      entries.subList(0, entriesToRemove).clear();

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

      if (fromIndex <= lastIncludedIndex || fromIndex > getLastLogIndex() + 1L) {
        throw new IllegalArgumentException("Invalid replacement index");
      }

      int start = (int) (fromIndex - lastIncludedIndex - 1);

      entries.subList(start, entries.size()).clear();

      appendAll(replacements);
    }

    @Override
    public synchronized void close() {}
  }
}
