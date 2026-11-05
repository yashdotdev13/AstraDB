package io.astradb.storage.rocksdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestamp;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for the WAL-backed RocksDB MVCC store. */
class WalBackedRocksDbMvccStoreTests {

  @TempDir Path tempDir;

  private static final String KEY = "test-key";

  private static HybridTimestamp timestamp(long physicalTime) {
    return new HybridTimestamp(physicalTime, 0);
  }

  private WalBackedRocksDbMvccStore<String, String> openStore() throws Exception {

    return WalBackedRocksDbMvccStore.open(
        tempDir.resolve("rocksdb"),
        tempDir.resolve("store.wal"),
        new StringKeySerializer(),
        new StringValueSerializer());
  }

  @Test
  void shouldPutAndGetValue() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(KEY, "value-1", timestamp(1));

      assertEquals("value-1", store.get(KEY, timestamp(1)).orElseThrow());
    }
  }

  @Test
  void shouldReturnEmptyForMissingKey() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      assertTrue(store.get("missing-key", timestamp(1)).isEmpty());
    }
  }

  @Test
  void shouldRecoverCommittedValueAfterReopening() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(KEY, "persisted-value", timestamp(1));
      store.flush();
    }

    try (WalBackedRocksDbMvccStore<String, String> reopenedStore = openStore()) {

      assertEquals("persisted-value", reopenedStore.get(KEY, timestamp(1)).orElseThrow());
    }
  }

  @Test
  void shouldRecoverMultipleMvccVersionsAfterReopening() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(KEY, "version-1", timestamp(1));
      store.put(KEY, "version-2", timestamp(2));
      store.put(KEY, "version-3", timestamp(3));
      store.flush();
    }

    try (WalBackedRocksDbMvccStore<String, String> reopenedStore = openStore()) {

      assertEquals("version-1", reopenedStore.get(KEY, timestamp(1)).orElseThrow());
      assertEquals("version-2", reopenedStore.get(KEY, timestamp(2)).orElseThrow());
      assertEquals("version-3", reopenedStore.get(KEY, timestamp(3)).orElseThrow());
    }
  }

  @Test
  void shouldRecoverTombstoneAfterReopening() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(KEY, "original-value", timestamp(1));
      store.delete(KEY, timestamp(2));
      store.flush();
    }

    try (WalBackedRocksDbMvccStore<String, String> reopenedStore = openStore()) {

      assertEquals("original-value", reopenedStore.get(KEY, timestamp(1)).orElseThrow());
      assertTrue(reopenedStore.get(KEY, timestamp(2)).isEmpty());
    }
  }

  @Test
  void shouldNotDuplicateVersionsDuringRepeatedRecovery() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(KEY, "version-1", timestamp(1));
      store.put(KEY, "version-2", timestamp(2));
      store.flush();
    }

    // First reopen replays the WAL into the existing RocksDB database.
    try (WalBackedRocksDbMvccStore<String, String> firstReopen = openStore()) {

      assertEquals("version-2", firstReopen.get(KEY, timestamp(2)).orElseThrow());
      firstReopen.flush();
    }

    // Second reopen must not insert duplicate MVCC timestamps.
    try (WalBackedRocksDbMvccStore<String, String> secondReopen = openStore()) {

      assertEquals("version-1", secondReopen.get(KEY, timestamp(1)).orElseThrow());
      assertEquals("version-2", secondReopen.get(KEY, timestamp(2)).orElseThrow());
    }
  }

  @Test
  void shouldRecoverDeleteWithoutRestoringDeletedValue() throws Exception {

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(KEY, "value-1", timestamp(1));
      store.delete(KEY, timestamp(2));
      store.put(KEY, "value-3", timestamp(3));
      store.flush();
    }

    try (WalBackedRocksDbMvccStore<String, String> reopenedStore = openStore()) {

      assertEquals("value-1", reopenedStore.get(KEY, timestamp(1)).orElseThrow());
      assertTrue(reopenedStore.get(KEY, timestamp(2)).isEmpty());
      assertEquals("value-3", reopenedStore.get(KEY, timestamp(3)).orElseThrow());
      assertFalse(reopenedStore.get(KEY, timestamp(3)).isEmpty());
    }
  }

  @Test
  void shouldRecoverUnicodeKeysAndValues() throws Exception {

    String unicodeKey = "数据库-🔑";
    String unicodeValue = "AstraDB-数据存储-🚀";

    try (WalBackedRocksDbMvccStore<String, String> store = openStore()) {

      store.put(unicodeKey, unicodeValue, timestamp(1));
      store.flush();
    }

    try (WalBackedRocksDbMvccStore<String, String> reopenedStore = openStore()) {

      assertEquals(unicodeValue, reopenedStore.get(unicodeKey, timestamp(1)).orElseThrow());
    }
  }
}
