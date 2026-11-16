package io.astradb.raft.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryRaftSnapshotStoreTests {

  private InMemoryRaftSnapshotStore store;

  @BeforeEach
  void setUp() {
    store = new InMemoryRaftSnapshotStore();
  }

  @Test
  void shouldReturnEmptyWhenNoSnapshotExists() {
    assertTrue(store.load().isEmpty());
  }

  @Test
  void shouldSaveAndLoadSnapshot() {
    RaftSnapshot snapshot = snapshot(5, 2, "state");

    store.save(snapshot);

    assertEquals(snapshot, store.load().orElseThrow());
  }

  @Test
  void shouldRejectNullSnapshot() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  @Test
  void shouldRejectOlderSnapshot() {
    store.save(snapshot(10, 3, "newer"));

    assertThrows(IllegalArgumentException.class, () -> store.save(snapshot(5, 2, "older")));

    assertEquals(10, store.load().orElseThrow().getLastIncludedIndex());
  }

  @Test
  void shouldAllowReplacingSnapshotAtSameIndex() {
    store.save(snapshot(10, 3, "original"));
    RaftSnapshot replacement = snapshot(10, 3, "replacement");

    store.save(replacement);

    assertEquals(replacement, store.load().orElseThrow());
  }

  @Test
  void shouldReplaceSnapshotWithNewerSnapshot() {
    store.save(snapshot(5, 2, "older"));
    RaftSnapshot newer = snapshot(10, 3, "newer");

    store.save(newer);

    assertEquals(newer, store.load().orElseThrow());
  }

  @Test
  void shouldDeleteSnapshot() {
    store.save(snapshot(5, 2, "state"));

    store.delete();

    assertTrue(store.load().isEmpty());
  }

  @Test
  void shouldRemainEmptyWhenDeletingMissingSnapshot() {
    assertDoesNotThrow(() -> store.delete());
    assertTrue(store.load().isEmpty());
  }

  @Test
  void shouldRejectOperationsAfterClose() {
    store.save(snapshot(5, 2, "state"));
    store.close();

    assertThrows(IllegalStateException.class, () -> store.save(snapshot(6, 2, "next")));
    assertThrows(IllegalStateException.class, () -> store.load());
    assertThrows(IllegalStateException.class, () -> store.delete());
  }

  @Test
  void shouldAllowClosingMultipleTimes() {
    store.close();

    assertDoesNotThrow(() -> store.close());
  }

  private RaftSnapshot snapshot(long index, long term, String data) {
    return new RaftSnapshot(index, term, data.getBytes(), Instant.parse("2026-11-16T10:00:00Z"));
  }
}
