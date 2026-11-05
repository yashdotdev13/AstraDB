package io.astradb.storage.rocksdb;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

class RocksDbMvccStoreTests {

  @TempDir Path tempDir;

  private RocksDbMvccStore<String, String> openStore(Path path) throws RocksDBException {
    return RocksDbMvccStore.open(path, new StringKeySerializer(), new StringValueSerializer());
  }

  private HybridTimestamp timestamp(long physicalTime) {
    return new HybridTimestamp(physicalTime, 0);
  }

  @Test
  void shouldStoreAndRetrieveValue() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      store.put("name", "AstraDB", timestamp(100));

      assertEquals("AstraDB", store.get("name", timestamp(100)));
    }
  }

  @Test
  void shouldReturnNullForMissingKey() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      assertNull(store.get("missing", timestamp(100)));
    }
  }

  @Test
  void shouldRespectMvccSnapshotVisibility() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      store.put("key", "v1", timestamp(100));
      store.put("key", "v2", timestamp(200));

      assertEquals("v1", store.get("key", timestamp(100)));
      assertEquals("v1", store.get("key", timestamp(150)));
      assertEquals("v2", store.get("key", timestamp(200)));
      assertEquals("v2", store.get("key", timestamp(300)));
    }
  }

  @Test
  void shouldRespectTombstoneVisibility() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      store.put("key", "value", timestamp(100));
      store.delete("key", timestamp(200));

      assertEquals("value", store.get("key", timestamp(150)));
      assertNull(store.get("key", timestamp(200)));
      assertNull(store.get("key", timestamp(300)));
    }
  }

  @Test
  void shouldPersistDataAcrossReopen() throws RocksDBException {
    Path databasePath = tempDir.resolve("persistent-db");

    try (RocksDbMvccStore<String, String> store = openStore(databasePath)) {
      store.put("key", "persistent-value", timestamp(100));
      store.flush();
    }

    try (RocksDbMvccStore<String, String> reopenedStore = openStore(databasePath)) {
      assertEquals("persistent-value", reopenedStore.get("key", timestamp(100)));
    }
  }

  @Test
  void shouldPreserveVersionHistoryAcrossReopen() throws RocksDBException {
    Path databasePath = tempDir.resolve("history-db");

    try (RocksDbMvccStore<String, String> store = openStore(databasePath)) {
      store.put("key", "v1", timestamp(100));
      store.put("key", "v2", timestamp(200));
      store.flush();
    }

    try (RocksDbMvccStore<String, String> reopenedStore = openStore(databasePath)) {
      assertEquals("v1", reopenedStore.get("key", timestamp(150)));
      assertEquals("v2", reopenedStore.get("key", timestamp(200)));
    }
  }

  @Test
  void shouldRejectDuplicateTimestamp() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      store.put("key", "v1", timestamp(100));

      assertThrows(IllegalArgumentException.class, () -> store.put("key", "v2", timestamp(100)));
    }
  }

  @Test
  void shouldSupportUnicodeKeysAndValues() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("unicode-db"))) {
      String key = "数据库-🚀";
      String value = "AstraDB 数据存储";

      store.put(key, value, timestamp(100));

      assertEquals(value, store.get(key, timestamp(100)));
    }
  }

  @Test
  void shouldRejectNullKey() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      assertThrows(NullPointerException.class, () -> store.put(null, "value", timestamp(100)));
      assertThrows(NullPointerException.class, () -> store.get(null, timestamp(100)));
      assertThrows(NullPointerException.class, () -> store.delete(null, timestamp(100)));
    }
  }

  @Test
  void shouldRejectNullValue() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      assertThrows(NullPointerException.class, () -> store.put("key", null, timestamp(100)));
    }
  }

  @Test
  void shouldRejectNullTimestamp() throws RocksDBException {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("db"))) {
      assertThrows(NullPointerException.class, () -> store.put("key", "value", null));
      assertThrows(NullPointerException.class, () -> store.get("key", null));
      assertThrows(NullPointerException.class, () -> store.delete("key", null));
    }
  }

  @Test
  void shouldPreserveAllVersionsDuringConcurrentWrites() throws Exception {
    try (RocksDbMvccStore<String, String> store = openStore(tempDir.resolve("concurrent-db"));
        ExecutorService executor = Executors.newFixedThreadPool(8)) {

      int numberOfWrites = 8;
      CountDownLatch startLatch = new CountDownLatch(1);
      Future<?>[] futures = new Future<?>[numberOfWrites];

      for (int i = 0; i < numberOfWrites; i++) {
        final int version = i;

        futures[i] =
            executor.submit(
                () -> {
                  startLatch.await();
                  store.put("shared-key", "value-" + version, timestamp(version + 1));
                  return null;
                });
      }

      startLatch.countDown();

      for (Future<?> future : futures) {
        future.get();
      }

      for (int i = 1; i <= numberOfWrites; i++) {
        assertNotNull(
            store.get("shared-key", timestamp(i)),
            "Version at timestamp " + i + " should be visible");
      }
    }
  }
}
