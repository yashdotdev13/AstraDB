package io.astradb.storage.rocksdb;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

class RocksDbStoreTests {

  @TempDir Path tempDir;

  @Test
  void shouldStoreAndRetrieveValue() throws RocksDBException {
    try (RocksDbStore store = RocksDbStore.open(tempDir.resolve("database"))) {
      store.put(bytes("key"), bytes("value"));

      assertArrayEquals(bytes("value"), store.get(bytes("key")));
    }
  }

  @Test
  void shouldReturnNullForMissingKey() throws RocksDBException {
    try (RocksDbStore store = RocksDbStore.open(tempDir.resolve("database"))) {
      assertNull(store.get(bytes("missing")));
    }
  }

  @Test
  void shouldDeleteValue() throws RocksDBException {
    try (RocksDbStore store = RocksDbStore.open(tempDir.resolve("database"))) {
      store.put(bytes("key"), bytes("value"));
      store.delete(bytes("key"));

      assertNull(store.get(bytes("key")));
    }
  }

  @Test
  void shouldPersistDataAfterReopening() throws RocksDBException {
    Path databasePath = tempDir.resolve("persistent-database");

    try (RocksDbStore store = RocksDbStore.open(databasePath)) {
      store.put(bytes("key"), bytes("persistent-value"));
    }

    try (RocksDbStore reopenedStore = RocksDbStore.open(databasePath)) {
      assertArrayEquals(bytes("persistent-value"), reopenedStore.get(bytes("key")));
    }
  }

  @Test
  void shouldRejectNullKey() throws RocksDBException {
    try (RocksDbStore store = RocksDbStore.open(tempDir.resolve("database"))) {
      assertThrows(NullPointerException.class, () -> store.get(null));
    }
  }

  @Test
  void shouldRejectNullValue() throws RocksDBException {
    try (RocksDbStore store = RocksDbStore.open(tempDir.resolve("database"))) {
      assertThrows(NullPointerException.class, () -> store.put(bytes("key"), null));
    }
  }

  @Test
  void shouldFlushSuccessfully() throws RocksDBException {
    try (RocksDbStore store = RocksDbStore.open(tempDir.resolve("database"))) {
      store.put(bytes("key"), bytes("value"));

      assertDoesNotThrow(store::flush);

      assertArrayEquals(bytes("value"), store.get(bytes("key")));
    }
  }

  @Test
  void shouldPersistSynchronousBatchWriteAfterReopening() throws RocksDBException {
    Path databasePath = tempDir.resolve("synchronous-database");

    try (RocksDbStore store = RocksDbStore.open(databasePath);
        WriteBatch batch = new WriteBatch()) {

      batch.put(bytes("raft:log:1"), bytes("entry-1"));
      batch.put(bytes("raft:log:2"), bytes("entry-2"));

      store.write(batch, true);
    }

    try (RocksDbStore reopenedStore = RocksDbStore.open(databasePath)) {
      assertArrayEquals(bytes("entry-1"), reopenedStore.get(bytes("raft:log:1")));
      assertArrayEquals(bytes("entry-2"), reopenedStore.get(bytes("raft:log:2")));
    }
  }

  private byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
