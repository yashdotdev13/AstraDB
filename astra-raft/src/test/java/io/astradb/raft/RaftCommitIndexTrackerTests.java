package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.raft.election.RaftCommitIndexTracker;
import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.state.RaftNodeState;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftCommitIndexTrackerTests {

  private RaftNodeState nodeState;
  private InMemoryLogStore logStore;
  private RaftCommitIndexTracker tracker;

  @BeforeEach
  void setUp() {
    nodeState = new RaftNodeState(new InMemoryMetadataStore());
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    logStore = new InMemoryLogStore();
    tracker = new RaftCommitIndexTracker(nodeState, logStore, 3);
  }

  @Test
  void shouldAdvanceCommitIndexWhenMajorityReplicatesCurrentTermEntry() {
    appendEntries(1, 1, 1, 1, 1);

    long commitIndex = tracker.advanceCommitIndex(List.of(5L, 4L, 2L));

    assertEquals(4, commitIndex);
    assertEquals(4, tracker.getCommitIndex());
  }

  @Test
  void shouldNotAdvanceCommitIndexWithoutMajority() {
    appendEntries(1, 1, 1, 1, 1);

    long commitIndex = tracker.advanceCommitIndex(List.of(5L, 0L, 0L));

    assertEquals(0, commitIndex);
    assertEquals(0, tracker.getCommitIndex());
  }

  @Test
  void shouldNotCommitEntryFromAnOlderTerm() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    appendEntries(1, 1, 1, 1, 1);

    long commitIndex = tracker.advanceCommitIndex(List.of(5L, 5L, 5L));

    assertEquals(0, commitIndex);
  }

  @Test
  void shouldNotMoveCommitIndexBackwards() {
    appendEntries(1, 1, 1, 1, 1);

    tracker.advanceCommitIndex(List.of(5L, 4L, 2L));

    long commitIndex = tracker.advanceCommitIndex(List.of(2L, 2L, 2L));

    assertEquals(4, commitIndex);
  }

  @Test
  void shouldCalculateMajorityForFiveNodeCluster() {
    RaftCommitIndexTracker fiveNodeTracker = new RaftCommitIndexTracker(nodeState, logStore, 5);

    appendEntries(1, 1, 1, 1, 1);

    long commitIndex = fiveNodeTracker.advanceCommitIndex(List.of(5L, 4L, 3L, 2L, 1L));

    assertEquals(3, commitIndex);
  }

  @Test
  void shouldRejectIncorrectNumberOfReplicationIndexes() {
    assertThrows(IllegalArgumentException.class, () -> tracker.advanceCommitIndex(List.of(3L, 2L)));
  }

  @Test
  void shouldRejectNegativeReplicationIndex() {
    assertThrows(
        IllegalArgumentException.class, () -> tracker.advanceCommitIndex(List.of(3L, 2L, -1L)));
  }

  @Test
  void shouldRejectNonPositiveClusterSize() {
    assertThrows(
        IllegalArgumentException.class, () -> new RaftCommitIndexTracker(nodeState, logStore, 0));
  }

  @Test
  void shouldRejectNullDependencies() {
    assertThrows(NullPointerException.class, () -> new RaftCommitIndexTracker(null, logStore, 3));

    assertThrows(NullPointerException.class, () -> new RaftCommitIndexTracker(nodeState, null, 3));
  }

  private void appendEntries(long... terms) {
    List<RaftLogEntry> entries = new ArrayList<>();

    for (int i = 0; i < terms.length; i++) {
      entries.add(
          new RaftLogEntry(
              i + 1L,
              terms[i],
              ("entry-" + (i + 1)).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    logStore.appendAll(entries);
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

    @Override
    public synchronized void append(RaftLogEntry entry) {
      appendAll(List.of(entry));
    }

    @Override
    public synchronized void appendAll(List<RaftLogEntry> newEntries) {
      for (RaftLogEntry entry : newEntries) {
        if (entry.getIndex() != entries.size() + 1L) {
          throw new IllegalArgumentException("Non-sequential log entry");
        }
        entries.add(entry);
      }
    }

    @Override
    public synchronized Optional<RaftLogEntry> getEntry(long index) {
      if (index <= 0 || index > entries.size()) {
        return Optional.empty();
      }
      return Optional.of(entries.get((int) index - 1));
    }

    @Override
    public synchronized List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      if (fromIndex <= 0 || toIndex < fromIndex || toIndex > entries.size()) {
        throw new IllegalArgumentException("Invalid log range");
      }
      return List.copyOf(entries.subList((int) fromIndex - 1, (int) toIndex));
    }

    @Override
    public synchronized long getLastLogIndex() {
      return entries.size();
    }

    @Override
    public synchronized long getLastLogTerm() {
      return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).getTerm();
    }

    @Override
    public synchronized void truncateSuffix(long fromIndex) {
      replaceSuffix(fromIndex, List.of());
    }

    @Override
    public synchronized void replaceSuffix(long fromIndex, List<RaftLogEntry> replacements) {
      if (fromIndex <= 0 || fromIndex > entries.size() + 1L) {
        throw new IllegalArgumentException("Invalid replacement index");
      }
      entries.subList((int) fromIndex - 1, entries.size()).clear();
      appendAll(replacements);
    }

    @Override
    public synchronized void flush() {}

    @Override
    public synchronized void close() {}
  }
}
