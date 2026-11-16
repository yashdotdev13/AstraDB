package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.InstallSnapshotHandler;
import io.astradb.raft.rpc.InstallSnapshotRequest;
import io.astradb.raft.rpc.InstallSnapshotResponse;
import io.astradb.raft.snapshot.InMemoryRaftSnapshotStore;
import io.astradb.raft.snapshot.RaftSnapshot;
import io.astradb.raft.snapshot.RaftSnapshotStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import io.astradb.raft.state.RaftStateMachine;
import io.astradb.raft.state.RaftStateMachineApplier;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InstallSnapshotHandlerTests {

  private InMemoryMetadataStore metadataStore;
  private RaftNodeState nodeState;
  private InMemoryLogStore logStore;
  private InMemoryRaftSnapshotStore snapshotStore;
  private RaftCommitIndex commitIndex;
  private AtomicInteger timeoutResetCount;
  private AtomicInteger restoreCount;
  private byte[] restoredData;
  private RaftStateMachineApplier applier;
  private InstallSnapshotHandler handler;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    logStore = new InMemoryLogStore();
    snapshotStore = new InMemoryRaftSnapshotStore();
    commitIndex = new RaftCommitIndex();
    timeoutResetCount = new AtomicInteger();
    restoreCount = new AtomicInteger();

    RaftStateMachine stateMachine =
        new RaftStateMachine() {
          @Override
          public void apply(long index, byte[] command) {}

          @Override
          public void restore(byte[] snapshotData) {
            restoreCount.incrementAndGet();
            restoredData = snapshotData.clone();
          }
        };

    applier = new RaftStateMachineApplier(logStore, commitIndex, stateMachine);

    handler =
        new InstallSnapshotHandler(
            nodeState, logStore, snapshotStore, timeoutResetCount::incrementAndGet, applier);
  }

  @Test
  void shouldInstallSnapshotAndRestoreStateMachine() {
    byte[] data = bytes("snapshot-data");

    InstallSnapshotResponse response = handler.handle(request(2, 5, 2, data));

    assertEquals(2, response.getTerm());
    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(5, logStore.getLastIncludedIndex());
    assertEquals(2, logStore.getLastIncludedTerm());
    assertEquals(5, commitIndex.getCommitIndex());
    assertEquals(5, applier.getLastApplied());
    assertEquals(1, restoreCount.get());
    assertArrayEquals(data, restoredData);

    RaftSnapshot persistedSnapshot = snapshotStore.load().orElseThrow();
    assertEquals(5, persistedSnapshot.getLastIncludedIndex());
    assertEquals(2, persistedSnapshot.getLastIncludedTerm());
    assertArrayEquals(data, persistedSnapshot.getData());
  }

  @Test
  void shouldRejectRequestFromStaleTerm() {
    nodeState.advanceTerm(2);

    InstallSnapshotResponse response = handler.handle(request(1, 5, 1, bytes("snapshot")));

    assertEquals(2, response.getTerm());
    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(0, timeoutResetCount.get());
    assertEquals(0, restoreCount.get());
    assertTrue(snapshotStore.load().isEmpty());
    assertEquals(0, logStore.getLastIncludedIndex());
  }

  @Test
  void shouldAdvanceTermAndBecomeFollowerForHigherTerm() {
    nodeState.startElection("node-1");

    InstallSnapshotResponse response = handler.handle(request(2, 5, 1, bytes("snapshot")));

    assertEquals(2, response.getTerm());
    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(1, restoreCount.get());
  }

  @Test
  void shouldBecomeFollowerForSameTermLeaderRequest() {
    nodeState.startElection("node-1");

    InstallSnapshotResponse response = handler.handle(request(1, 5, 1, bytes("snapshot")));

    assertEquals(1, response.getTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertEquals(1, timeoutResetCount.get());
  }

  @Test
  void shouldResetTimeoutWhenSnapshotIsAlreadyCommitted() {
    commitIndex.advanceTo(5);

    InstallSnapshotResponse response = handler.handle(request(2, 5, 2, bytes("snapshot")));

    assertEquals(2, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, restoreCount.get());
    assertTrue(snapshotStore.load().isEmpty());
    assertEquals(0, logStore.getLastIncludedIndex());
  }

  @Test
  void shouldIgnoreSnapshotAlreadyCoveredByLogSnapshotBoundary() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two")));
    logStore.installSnapshotBoundary(2, 1);

    InstallSnapshotResponse response = handler.handle(request(1, 2, 1, bytes("old-snapshot")));

    assertEquals(1, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, restoreCount.get());
    assertTrue(snapshotStore.load().isEmpty());
    assertEquals(2, logStore.getLastIncludedIndex());
  }

  @Test
  void shouldPreserveMatchingLogSuffix() {
    logStore.appendAll(
        List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three"), entry(4, 2, "four")));

    handler.handle(request(2, 2, 1, bytes("snapshot")));

    assertEquals(2, logStore.getLastIncludedIndex());
    assertEquals(1, logStore.getLastIncludedTerm());
    assertEquals(4, logStore.getLastLogIndex());
    assertTrue(logStore.getEntry(1).isEmpty());
    assertTrue(logStore.getEntry(2).isEmpty());
    assertTrue(logStore.getEntry(3).isPresent());
    assertTrue(logStore.getEntry(4).isPresent());
  }

  @Test
  void shouldDiscardConflictingLogSuffix() {
    logStore.appendAll(List.of(entry(1, 1, "one"), entry(2, 2, "two"), entry(3, 2, "three")));

    handler.handle(request(2, 2, 1, bytes("snapshot")));

    assertEquals(2, logStore.getLastIncludedIndex());
    assertEquals(1, logStore.getLastIncludedTerm());
    assertEquals(2, logStore.getLastLogIndex());
    assertTrue(logStore.getEntry(1).isEmpty());
    assertTrue(logStore.getEntry(2).isEmpty());
    assertTrue(logStore.getEntry(3).isEmpty());
  }

  @Test
  void shouldPropagateSnapshotPersistenceFailureWithoutRestoringStateMachine() {
    RaftSnapshotStore failingStore =
        new RaftSnapshotStore() {
          @Override
          public void save(RaftSnapshot snapshot) {
            throw new IllegalStateException("Snapshot persistence failed");
          }

          @Override
          public Optional<RaftSnapshot> load() {
            return Optional.empty();
          }

          @Override
          public void delete() {}

          @Override
          public void close() {}
        };

    InstallSnapshotHandler failingHandler =
        new InstallSnapshotHandler(
            nodeState, logStore, failingStore, timeoutResetCount::incrementAndGet, applier);

    assertThrows(
        IllegalStateException.class,
        () -> failingHandler.handle(request(2, 5, 2, bytes("snapshot"))));

    assertEquals(0, restoreCount.get());
    assertEquals(0, logStore.getLastIncludedIndex());
    assertEquals(0, commitIndex.getCommitIndex());
    assertEquals(0, applier.getLastApplied());
  }

  @Test
  void shouldNotInstallLogBoundaryWhenStateMachineRestorationFails() {
    RaftStateMachine failingStateMachine =
        new RaftStateMachine() {
          @Override
          public void apply(long index, byte[] command) {}

          @Override
          public void restore(byte[] snapshotData) {
            throw new IllegalStateException("State machine restoration failed");
          }
        };

    RaftStateMachineApplier failingApplier =
        new RaftStateMachineApplier(logStore, commitIndex, failingStateMachine);

    InstallSnapshotHandler failingHandler =
        new InstallSnapshotHandler(
            nodeState, logStore, snapshotStore, timeoutResetCount::incrementAndGet, failingApplier);

    assertThrows(
        IllegalStateException.class,
        () -> failingHandler.handle(request(2, 5, 2, bytes("snapshot"))));

    assertTrue(snapshotStore.load().isPresent());
    assertEquals(0, logStore.getLastIncludedIndex());
    assertEquals(0, commitIndex.getCommitIndex());
    assertEquals(0, failingApplier.getLastApplied());
  }

  @Test
  void shouldIgnoreSnapshotOlderThanPersistedSnapshot() {
    byte[] existingData = bytes("newer-snapshot");

    snapshotStore.save(new RaftSnapshot(8, 1, existingData, Instant.now()));

    InstallSnapshotResponse response = handler.handle(request(2, 6, 1, bytes("older-snapshot")));

    assertEquals(2, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, restoreCount.get());
    assertEquals(0, logStore.getLastIncludedIndex());

    RaftSnapshot persisted = snapshotStore.load().orElseThrow();
    assertEquals(8, persisted.getLastIncludedIndex());
    assertEquals(1, persisted.getLastIncludedTerm());
    assertArrayEquals(existingData, persisted.getData());
  }

  @Test
  void shouldIgnoreDuplicatePersistedSnapshot() {
    byte[] existingData = bytes("existing-snapshot");

    snapshotStore.save(new RaftSnapshot(5, 1, existingData, Instant.now()));

    InstallSnapshotResponse response =
        handler.handle(request(2, 5, 1, bytes("duplicate-snapshot")));

    assertEquals(2, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, restoreCount.get());
    assertEquals(0, logStore.getLastIncludedIndex());

    RaftSnapshot persisted = snapshotStore.load().orElseThrow();
    assertEquals(5, persisted.getLastIncludedIndex());
    assertEquals(1, persisted.getLastIncludedTerm());
    assertArrayEquals(existingData, persisted.getData());
  }

  @Test
  void shouldIgnoreSnapshotWithConflictingTermAtSameIndex() {
    byte[] existingData = bytes("existing-snapshot");

    snapshotStore.save(new RaftSnapshot(5, 1, existingData, Instant.now()));

    InstallSnapshotResponse response =
        handler.handle(request(2, 5, 2, bytes("conflicting-snapshot")));

    assertEquals(2, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(0, restoreCount.get());
    assertEquals(0, logStore.getLastIncludedIndex());

    RaftSnapshot persisted = snapshotStore.load().orElseThrow();
    assertEquals(5, persisted.getLastIncludedIndex());
    assertEquals(1, persisted.getLastIncludedTerm());
    assertArrayEquals(existingData, persisted.getData());
  }

  @Test
  void shouldReplaceOlderPersistedSnapshotWithNewerSnapshot() {
    snapshotStore.save(new RaftSnapshot(5, 1, bytes("old-snapshot"), Instant.now()));

    byte[] newData = bytes("new-snapshot");

    InstallSnapshotResponse response = handler.handle(request(2, 8, 2, newData));

    assertEquals(2, response.getTerm());
    assertEquals(1, timeoutResetCount.get());
    assertEquals(1, restoreCount.get());
    assertArrayEquals(newData, restoredData);

    assertEquals(8, logStore.getLastIncludedIndex());
    assertEquals(2, logStore.getLastIncludedTerm());
    assertEquals(8, commitIndex.getCommitIndex());
    assertEquals(8, applier.getLastApplied());

    RaftSnapshot persisted = snapshotStore.load().orElseThrow();
    assertEquals(8, persisted.getLastIncludedIndex());
    assertEquals(2, persisted.getLastIncludedTerm());
    assertArrayEquals(newData, persisted.getData());
  }

  private InstallSnapshotRequest request(
      long term, long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
    return new InstallSnapshotRequest(term, "leader-1", lastIncludedIndex, lastIncludedTerm, data);
  }

  private RaftLogEntry entry(long index, long term, String payload) {
    return new RaftLogEntry(index, term, bytes(payload));
  }

  private byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
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
        long expectedIndex = getLastLogIndex() + 1;

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
    public synchronized Optional<Long> getTerm(long index) {
      if (index == lastIncludedIndex && index > 0) {
        return Optional.of(lastIncludedTerm);
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
    public synchronized void installSnapshotBoundary(long index, long term) {
      if (index <= 0 || term <= 0) {
        throw new IllegalArgumentException("Invalid snapshot boundary");
      }

      if (index < lastIncludedIndex) {
        throw new IllegalArgumentException("Snapshot boundary cannot move backwards");
      }

      if (index == lastIncludedIndex) {
        if (term != lastIncludedTerm) {
          throw new IllegalArgumentException("Snapshot boundary term mismatch");
        }
        return;
      }

      RaftLogEntry boundaryEntry = getEntry(index).orElse(null);
      boolean preserveSuffix = boundaryEntry != null && boundaryEntry.getTerm() == term;

      if (preserveSuffix) {
        int entriesToRemove = (int) (index - lastIncludedIndex);
        entries.subList(0, entriesToRemove).clear();
      } else {
        entries.clear();
      }

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

      int start = (int) (fromIndex - lastIncludedIndex - 1);
      entries.subList(start, entries.size()).clear();
      appendAll(replacements);
    }

    @Override
    public synchronized void close() {}
  }
}
