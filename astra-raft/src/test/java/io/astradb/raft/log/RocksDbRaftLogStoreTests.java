package io.astradb.raft.log;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDbRaftLogStoreTests {

  @TempDir Path tempDir;

  @Test
  void shouldAppendAndRetrieveEntry() {
    try (RocksDbRaftLogStore store = open("append-db")) {
      RaftLogEntry entry = entry(1, 1, "command-1");

      store.append(entry);

      RaftLogEntry retrieved = store.getEntry(1).orElseThrow();

      assertEquals(entry.getIndex(), retrieved.getIndex());
      assertEquals(entry.getTerm(), retrieved.getTerm());
      assertArrayEquals(entry.getPayload(), retrieved.getPayload());
      assertEquals(1, store.getLastLogIndex());
      assertEquals(1, store.getLastLogTerm());
    }
  }

  @Test
  void shouldAppendMultipleEntriesAtomically() {
    try (RocksDbRaftLogStore store = open("batch-db")) {
      store.appendAll(
          List.of(entry(1, 1, "command-1"), entry(2, 1, "command-2"), entry(3, 2, "command-3")));

      assertEquals(3, store.getLastLogIndex());
      assertEquals(2, store.getLastLogTerm());
      assertEquals(3, store.getEntries(1, 3).size());
    }
  }

  @Test
  void shouldRejectNonSequentialAppend() {
    try (RocksDbRaftLogStore store = open("sequence-db")) {
      assertThrows(IllegalArgumentException.class, () -> store.append(entry(2, 1, "command-2")));

      assertEquals(0, store.getLastLogIndex());
    }
  }

  @Test
  void shouldReturnEmptyForMissingEntry() {
    try (RocksDbRaftLogStore store = open("missing-db")) {
      assertTrue(store.getEntry(1).isEmpty());
    }
  }

  @Test
  void shouldRetrieveEntriesInInclusiveRange() {
    try (RocksDbRaftLogStore store = open("range-db")) {
      store.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

      List<RaftLogEntry> entries = store.getEntries(2, 3);

      assertEquals(2, entries.size());
      assertEquals(2, entries.get(0).getIndex());
      assertEquals(3, entries.get(1).getIndex());
    }
  }

  @Test
  void shouldRejectRangeBeyondLastLogIndex() {
    try (RocksDbRaftLogStore store = open("invalid-range-db")) {
      store.append(entry(1, 1, "one"));

      assertThrows(IllegalArgumentException.class, () -> store.getEntries(1, 2));
    }
  }

  @Test
  void shouldTruncateLogSuffix() {
    try (RocksDbRaftLogStore store = open("truncate-db")) {
      store.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

      store.truncateSuffix(2);

      assertEquals(1, store.getLastLogIndex());
      assertTrue(store.getEntry(2).isEmpty());
      assertTrue(store.getEntry(3).isEmpty());
      assertEquals(1, store.getLastLogTerm());
    }
  }

  @Test
  void shouldPersistEntriesAfterReopening() {
    Path databasePath = tempDir.resolve("persistent-db");

    try (RocksDbRaftLogStore store = RocksDbRaftLogStore.open(databasePath)) {
      store.appendAll(List.of(entry(1, 1, "one"), entry(2, 2, "two")));
    }

    try (RocksDbRaftLogStore reopenedStore = RocksDbRaftLogStore.open(databasePath)) {

      assertEquals(2, reopenedStore.getLastLogIndex());
      assertEquals(2, reopenedStore.getLastLogTerm());

      assertArrayEquals(bytes("one"), reopenedStore.getEntry(1).orElseThrow().getPayload());

      assertArrayEquals(bytes("two"), reopenedStore.getEntry(2).orElseThrow().getPayload());
    }
  }

  @Test
  void shouldPersistTruncationAfterReopening() {
    Path databasePath = tempDir.resolve("truncated-persistent-db");

    try (RocksDbRaftLogStore store = RocksDbRaftLogStore.open(databasePath)) {
      store.appendAll(List.of(entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 2, "three")));

      store.truncateSuffix(2);
    }

    try (RocksDbRaftLogStore reopenedStore = RocksDbRaftLogStore.open(databasePath)) {

      assertEquals(1, reopenedStore.getLastLogIndex());
      assertTrue(reopenedStore.getEntry(2).isEmpty());
    }
  }

  @Test
  void shouldReturnZeroForEmptyLog() {
    try (RocksDbRaftLogStore store = open("empty-db")) {
      assertEquals(0, store.getLastLogIndex());
      assertEquals(0, store.getLastLogTerm());
    }
  }

  @Test
  void shouldAllowAppendingAfterTruncation() {
    try (RocksDbRaftLogStore store = open("replacement-db")) {
      store.appendAll(
          List.of(entry(1, 1, "one"), entry(2, 1, "old-two"), entry(3, 1, "old-three")));

      store.truncateSuffix(2);

      store.appendAll(List.of(entry(2, 2, "new-two"), entry(3, 2, "new-three")));

      assertEquals(3, store.getLastLogIndex());
      assertArrayEquals(bytes("new-two"), store.getEntry(2).orElseThrow().getPayload());
    }
  }

  @Test
  void shouldRecoverLastLogIndexAndTermAfterReopening(@TempDir Path tempDir) {
    Path databasePath = tempDir.resolve("raft-recovery-test");

    RocksDbRaftLogStore store = RocksDbRaftLogStore.open(databasePath);

    store.append(new RaftLogEntry(1, 1, new byte[] {1}));
    store.append(new RaftLogEntry(2, 3, new byte[] {2}));
    store.close();

    RocksDbRaftLogStore reopened = RocksDbRaftLogStore.open(databasePath);

    assertEquals(2, reopened.getLastLogIndex());
    assertEquals(3, reopened.getLastLogTerm());

    reopened.close();
  }

  @Test
  void shouldReplaceConflictingSuffix() {
    RocksDbRaftLogStore store = RocksDbRaftLogStore.open(tempDir.resolve("replace-test"));

    store.appendAll(
        List.of(
            new RaftLogEntry(1, 1, new byte[] {1}),
            new RaftLogEntry(2, 1, new byte[] {2}),
            new RaftLogEntry(3, 2, new byte[] {3})));

    store.replaceSuffix(
        2,
        List.of(new RaftLogEntry(2, 3, new byte[] {20}), new RaftLogEntry(3, 3, new byte[] {30})));

    assertEquals(3, store.getLastLogIndex());
    assertEquals(1, store.getEntry(1).orElseThrow().getTerm());
    assertEquals(3, store.getEntry(2).orElseThrow().getTerm());
    assertEquals(3, store.getEntry(3).orElseThrow().getTerm());

    store.close();
  }

  @Test
  void shouldRemoveSuffixWhenReplacementEntriesAreEmpty() {
    RocksDbRaftLogStore store = RocksDbRaftLogStore.open(tempDir.resolve("empty-replacement-test"));

    store.appendAll(
        List.of(
            new RaftLogEntry(1, 1, new byte[] {1}),
            new RaftLogEntry(2, 1, new byte[] {2}),
            new RaftLogEntry(3, 2, new byte[] {3})));

    store.replaceSuffix(2, List.of());

    assertEquals(1, store.getLastLogIndex());
    assertTrue(store.getEntry(2).isEmpty());
    assertTrue(store.getEntry(3).isEmpty());

    store.close();
  }

  @Test
  void shouldRejectNonconsecutiveReplacementEntries() {
    RocksDbRaftLogStore store =
        RocksDbRaftLogStore.open(tempDir.resolve("invalid-replacement-test"));

    store.append(new RaftLogEntry(1, 1, new byte[] {1}));

    assertThrows(
        IllegalArgumentException.class,
        () -> store.replaceSuffix(2, List.of(new RaftLogEntry(3, 2, new byte[] {3}))));

    assertEquals(1, store.getLastLogIndex());

    store.close();
  }

  private RocksDbRaftLogStore open(String databaseName) {
    return RocksDbRaftLogStore.open(tempDir.resolve(databaseName));
  }

  private RaftLogEntry entry(long index, long term, String payload) {
    return new RaftLogEntry(index, term, bytes(payload));
  }

  private byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
