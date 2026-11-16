package io.astradb.raft.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDbRaftSnapshotStoreTests {

  @TempDir Path tempDir;

  @Test
  void shouldSaveAndLoadSnapshot() {
    try (RocksDbRaftSnapshotStore store = open("save-db")) {
      RaftSnapshot snapshot = snapshot(10, 3, "state");

      store.save(snapshot);

      assertEquals(snapshot, store.load().orElseThrow());
    }
  }

  @Test
  void shouldReturnEmptyWhenSnapshotDoesNotExist() {
    try (RocksDbRaftSnapshotStore store = open("empty-db")) {
      assertTrue(store.load().isEmpty());
    }
  }

  @Test
  void shouldPersistSnapshotAfterReopening() {
    Path databasePath = tempDir.resolve("persistent-db");
    RaftSnapshot snapshot = snapshot(10, 3, "persistent-state");

    try (RocksDbRaftSnapshotStore store = RocksDbRaftSnapshotStore.open(databasePath)) {
      store.save(snapshot);
    }

    try (RocksDbRaftSnapshotStore reopened = RocksDbRaftSnapshotStore.open(databasePath)) {
      assertEquals(snapshot, reopened.load().orElseThrow());
    }
  }

  @Test
  void shouldRejectOlderSnapshot() {
    try (RocksDbRaftSnapshotStore store = open("older-db")) {
      store.save(snapshot(10, 3, "newer"));

      assertThrows(IllegalArgumentException.class, () -> store.save(snapshot(5, 2, "older")));

      assertEquals(10, store.load().orElseThrow().getLastIncludedIndex());
    }
  }

  @Test
  void shouldAllowReplacingSnapshotAtSameIndex() {
    try (RocksDbRaftSnapshotStore store = open("same-index-db")) {
      store.save(snapshot(10, 3, "original"));
      RaftSnapshot replacement = snapshot(10, 3, "replacement");

      store.save(replacement);

      assertEquals(replacement, store.load().orElseThrow());
    }
  }

  @Test
  void shouldReplaceSnapshotWithNewerSnapshot() {
    try (RocksDbRaftSnapshotStore store = open("newer-db")) {
      store.save(snapshot(5, 2, "older"));
      RaftSnapshot newer = snapshot(10, 3, "newer");

      store.save(newer);

      assertEquals(newer, store.load().orElseThrow());
    }
  }

  @Test
  void shouldDeleteSnapshot() {
    try (RocksDbRaftSnapshotStore store = open("delete-db")) {
      store.save(snapshot(5, 2, "state"));

      store.delete();

      assertTrue(store.load().isEmpty());
    }
  }

  @Test
  void shouldPersistDeletionAfterReopening() {
    Path databasePath = tempDir.resolve("delete-persistent-db");

    try (RocksDbRaftSnapshotStore store = RocksDbRaftSnapshotStore.open(databasePath)) {
      store.save(snapshot(5, 2, "state"));
      store.delete();
    }

    try (RocksDbRaftSnapshotStore reopened = RocksDbRaftSnapshotStore.open(databasePath)) {
      assertTrue(reopened.load().isEmpty());
    }
  }

  @Test
  void shouldRejectOperationsAfterClose() {
    RocksDbRaftSnapshotStore store = open("closed-db");
    store.close();

    assertThrows(IllegalStateException.class, () -> store.load());
    assertThrows(IllegalStateException.class, () -> store.delete());
    assertThrows(IllegalStateException.class, () -> store.save(snapshot(1, 1, "state")));
  }

  @Test
  void shouldRejectNullSnapshot() {
    try (RocksDbRaftSnapshotStore store = open("null-db")) {
      assertThrows(NullPointerException.class, () -> store.save(null));
    }
  }

  @Test
  void shouldRejectCorruptedSnapshotOnReopen() throws Exception {
    Path databasePath = tempDir.resolve("corrupted-db");

    try (RocksDbRaftSnapshotStore store = RocksDbRaftSnapshotStore.open(databasePath)) {
      store.save(snapshot(5, 2, "state"));
    }

    try (io.astradb.storage.rocksdb.RocksDbStore rawStore =
        io.astradb.storage.rocksdb.RocksDbStore.open(databasePath)) {
      rawStore.put(
          "raft:snapshot:latest".getBytes(StandardCharsets.UTF_8), new byte[] {0, 0, 0, 99});
    }

    assertThrows(RaftLogException.class, () -> RocksDbRaftSnapshotStore.open(databasePath));
  }

  private RocksDbRaftSnapshotStore open(String name) {
    return RocksDbRaftSnapshotStore.open(tempDir.resolve(name));
  }

  private RaftSnapshot snapshot(long index, long term, String data) {
    return new RaftSnapshot(
        index, term, data.getBytes(StandardCharsets.UTF_8), Instant.parse("2026-11-16T10:00:00Z"));
  }
}
