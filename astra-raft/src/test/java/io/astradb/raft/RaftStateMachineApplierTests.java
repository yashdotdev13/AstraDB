package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftStateMachineApplier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftStateMachineApplierTests {

  private InMemoryLogStore logStore;
  private RaftCommitIndex commitIndex;
  private List<String> appliedCommands;
  private RaftStateMachineApplier applier;

  @BeforeEach
  void setUp() {
    logStore = new InMemoryLogStore();
    commitIndex = new RaftCommitIndex();
    appliedCommands = new ArrayList<>();

    applier =
        new RaftStateMachineApplier(
            logStore,
            commitIndex,
            (index, command) ->
                appliedCommands.add(index + ":" + new String(command, StandardCharsets.UTF_8)));
  }

  @Test
  void shouldApplyCommittedEntriesInOrder() {
    logStore.appendAll(List.of(entry(1, "one"), entry(2, "two"), entry(3, "three")));
    commitIndex.advanceTo(3);

    long lastApplied = applier.applyCommittedEntries();

    assertEquals(3, lastApplied);
    assertEquals(List.of("1:one", "2:two", "3:three"), appliedCommands);
    assertEquals(3, applier.getLastApplied());
  }

  @Test
  void shouldNotApplyEntriesBeyondCommitIndex() {
    logStore.appendAll(List.of(entry(1, "one"), entry(2, "two")));
    commitIndex.advanceTo(1);

    applier.applyCommittedEntries();

    assertEquals(List.of("1:one"), appliedCommands);
    assertEquals(1, applier.getLastApplied());
  }

  @Test
  void shouldNotApplyTheSameEntryTwice() {
    logStore.append(entry(1, "one"));
    commitIndex.advanceTo(1);

    applier.applyCommittedEntries();
    applier.applyCommittedEntries();

    assertEquals(List.of("1:one"), appliedCommands);
    assertEquals(1, applier.getLastApplied());
  }

  @Test
  void shouldRetryEntryWhenStateMachineApplicationFails() {
    logStore.appendAll(List.of(entry(1, "one"), entry(2, "two")));
    commitIndex.advanceTo(2);

    List<Long> attempts = new ArrayList<>();

    RaftStateMachineApplier failingApplier =
        new RaftStateMachineApplier(
            logStore,
            commitIndex,
            (index, command) -> {
              attempts.add(index);
              if (index == 2 && attempts.stream().filter(value -> value == 2).count() == 1) {
                throw new IllegalStateException("Application failed");
              }
            });

    assertThrows(IllegalStateException.class, failingApplier::applyCommittedEntries);

    assertEquals(1, failingApplier.getLastApplied());

    assertEquals(2, failingApplier.applyCommittedEntries());
    assertEquals(List.of(1L, 2L, 2L), attempts);
  }

  private RaftLogEntry entry(long index, String payload) {
    return new RaftLogEntry(index, 1, payload.getBytes(StandardCharsets.UTF_8));
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
    public synchronized void flush() {}

    @Override
    public synchronized void replaceSuffix(long fromIndex, List<RaftLogEntry> replacements) {
      if (fromIndex <= 0 || fromIndex > entries.size() + 1L) {
        throw new IllegalArgumentException("Invalid replacement index");
      }
      entries.subList((int) fromIndex - 1, entries.size()).clear();
      appendAll(replacements);
    }

    @Override
    public synchronized void close() {}
  }
}
