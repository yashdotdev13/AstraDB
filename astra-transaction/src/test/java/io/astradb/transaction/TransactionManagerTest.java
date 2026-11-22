package io.astradb.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.common.time.TimestampOracle;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.rocksdb.KeySerializer;
import io.astradb.storage.rocksdb.StringKeySerializer;
import io.astradb.storage.rocksdb.StringValueSerializer;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for transaction lifecycle management. */
class TransactionManagerTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, String> store;
  private WalBackedRocksDbMvccStore<String, String> failureStore;
  private HybridTimestampOracle timestampOracle;
  private SnapshotManager snapshotManager;
  private TransactionManager<String, String> transactionManager;

  @BeforeEach
  void setUp() throws Exception {
    store =
        WalBackedRocksDbMvccStore.open(
            tempDir.resolve("rocksdb"),
            tempDir.resolve("store.wal"),
            new StringKeySerializer(),
            new StringValueSerializer());

    timestampOracle = new HybridTimestampOracle();
    snapshotManager = new SnapshotManager();
    transactionManager = new TransactionManager<>(store, timestampOracle, snapshotManager);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (failureStore != null) {
      failureStore.close();
    }

    if (store != null) {
      store.close();
    }
  }

  @Test
  void shouldBeginTransactionAndRegisterSnapshot() {
    Transaction<String, String> transaction = transactionManager.begin();

    assertEquals(TransactionState.ACTIVE, transaction.getState());
    assertEquals(
        transaction.getId(), transactionManager.getTransaction(transaction.getId()).getId());
    assertEquals(1, snapshotManager.size());
    assertEquals(
        transaction.getReadTimestamp(),
        snapshotManager.get(transaction.getId()).orElseThrow().getTimestamp());
  }

  @Test
  void shouldReadItsOwnStagedWrites() {
    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("key", "value");

    assertEquals("value", transaction.get("key").orElseThrow());
    assertTrue(transaction.get("missing-key").isEmpty());
  }

  @Test
  void shouldUseLatestStagedMutationForSameKey() {
    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("key", "first-value");
    transaction.put("key", "second-value");

    assertEquals("second-value", transaction.get("key").orElseThrow());

    transaction.delete("key");

    assertTrue(transaction.get("key").isEmpty());
  }

  @Test
  void shouldPersistStagedWritesOnCommit() throws Exception {
    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("key", "committed-value");
    transactionManager.commit(transaction.getId());

    assertEquals(TransactionState.COMMITTED, transaction.getState());
    assertEquals("committed-value", store.get("key", timestampOracle.peek()).orElseThrow());
    assertEquals(0, snapshotManager.size());
    assertThrows(
        TransactionException.class, () -> transactionManager.getTransaction(transaction.getId()));
  }

  @Test
  void shouldDiscardStagedWritesOnRollback() throws Exception {
    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("key", "uncommitted-value");
    transactionManager.rollback(transaction.getId());

    assertEquals(TransactionState.ABORTED, transaction.getState());
    assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
    assertEquals(0, snapshotManager.size());
  }

  @Test
  void shouldCommitEmptyTransaction() {
    Transaction<String, String> transaction = transactionManager.begin();

    transactionManager.commit(transaction.getId());

    assertEquals(TransactionState.COMMITTED, transaction.getState());
    assertEquals(0, snapshotManager.size());
  }

  @Test
  void shouldRejectCommitAfterTransactionHasCompleted() {
    Transaction<String, String> transaction = transactionManager.begin();

    transactionManager.commit(transaction.getId());

    assertThrows(TransactionException.class, () -> transactionManager.commit(transaction.getId()));
  }

  @Test
  void shouldRejectRollbackAfterTransactionHasCompleted() {
    Transaction<String, String> transaction = transactionManager.begin();

    transactionManager.rollback(transaction.getId());

    assertThrows(
        TransactionException.class, () -> transactionManager.rollback(transaction.getId()));
  }

  @Test
  void shouldRejectMutationsAfterCommit() {
    Transaction<String, String> transaction = transactionManager.begin();

    transactionManager.commit(transaction.getId());

    assertThrows(TransactionException.class, () -> transaction.put("key", "value"));
  }

  @Test
  void shouldRejectMutationsAfterRollback() {
    Transaction<String, String> transaction = transactionManager.begin();

    transactionManager.rollback(transaction.getId());

    assertThrows(TransactionException.class, () -> transaction.put("key", "value"));
  }

  @Test
  void shouldPreservePreviouslyCommittedValuesWhenLaterTransactionRollsBack() throws Exception {
    Transaction<String, String> first = transactionManager.begin();
    first.put("key", "committed-value");
    transactionManager.commit(first.getId());

    Transaction<String, String> second = transactionManager.begin();
    second.put("key", "uncommitted-value");
    transactionManager.rollback(second.getId());

    assertEquals("committed-value", store.get("key", timestampOracle.peek()).orElseThrow());
    assertTrue(snapshotManager.isEmpty());
  }

  @Test
  void shouldAbortTransactionWhenCommitTimestampCannotBeGenerated() throws Exception {
    TimestampOracle failingOracle =
        new TimestampOracle() {
          private int callCount;

          @Override
          public HybridTimestamp now() {
            callCount++;

            if (callCount == 1) {
              return timestampOracle.now();
            }

            throw new IllegalStateException("Timestamp generation failed");
          }

          @Override
          public HybridTimestamp update(HybridTimestamp remoteTimestamp) {
            return timestampOracle.update(remoteTimestamp);
          }

          @Override
          public HybridTimestamp peek() {
            return timestampOracle.peek();
          }
        };

    TransactionManager<String, String> manager =
        new TransactionManager<>(store, failingOracle, snapshotManager);

    Transaction<String, String> transaction = manager.begin();
    transaction.put("key", "value");

    assertThrows(TransactionException.class, () -> manager.commit(transaction.getId()));

    assertEquals(TransactionState.ABORTED, transaction.getState());
    assertTrue(snapshotManager.isEmpty());
    assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
  }

  @Test
  void shouldMarkTransactionInDoubtWhenStorageApplicationFails() throws Exception {
    KeySerializer<String> failingKeySerializer =
        new KeySerializer<>() {
          @Override
          public byte[] serialize(String key) {
            throw new IllegalStateException("Serialization failed");
          }

          @Override
          public String deserialize(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
          }
        };

    failureStore =
        WalBackedRocksDbMvccStore.open(
            tempDir.resolve("failure-rocksdb"),
            tempDir.resolve("failure-store.wal"),
            failingKeySerializer,
            new StringValueSerializer());

    TransactionManager<String, String> manager =
        new TransactionManager<>(failureStore, timestampOracle, snapshotManager);

    Transaction<String, String> transaction = manager.begin();
    transaction.put("key", "value");

    assertThrows(TransactionException.class, () -> manager.commit(transaction.getId()));

    assertEquals(TransactionState.IN_DOUBT, transaction.getState());
    assertEquals(1, snapshotManager.size());
    assertEquals(transaction.getId(), manager.getTransaction(transaction.getId()).getId());
    assertThrows(TransactionException.class, () -> manager.rollback(transaction.getId()));

    assertThrows(TransactionException.class, () -> transaction.put("another-key", "another-value"));

    assertEquals(TransactionState.IN_DOUBT, transaction.getState());
    assertEquals(1, snapshotManager.size());
    assertEquals(transaction.getId(), manager.getTransaction(transaction.getId()).getId());
  }

  @Test
  void shouldReadFromItsOriginalSnapshotAfterAnotherTransactionCommits() throws Exception {
    store.put("key", "original-value", timestampOracle.now());

    Transaction<String, String> first = transactionManager.begin();

    Transaction<String, String> second = transactionManager.begin();
    second.put("key", "updated-value");
    transactionManager.commit(second.getId());

    assertEquals("original-value", first.get("key").orElseThrow());

    Transaction<String, String> third = transactionManager.begin();
    assertEquals("updated-value", third.get("key").orElseThrow());

    transactionManager.rollback(first.getId());
    transactionManager.rollback(third.getId());
  }

  @Test
  void shouldPersistDeletesOnCommit() throws Exception {
    store.put("key", "existing-value", timestampOracle.now());

    Transaction<String, String> transaction = transactionManager.begin();

    assertEquals("existing-value", transaction.get("key").orElseThrow());

    transaction.delete("key");
    transactionManager.commit(transaction.getId());

    assertEquals(TransactionState.COMMITTED, transaction.getState());
    assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
    assertEquals(0, snapshotManager.size());
  }

  @Test
  void shouldAllowOnlyOneConcurrentCommit() throws Exception {
    Transaction<String, String> transaction = transactionManager.begin();
    transaction.put("key", "value");

    CountDownLatch start = new CountDownLatch(1);

    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      Future<Boolean> first =
          executor.submit(
              () -> {
                await(start);
                try {
                  transactionManager.commit(transaction.getId());
                  return true;
                } catch (TransactionException exception) {
                  return false;
                }
              });

      Future<Boolean> second =
          executor.submit(
              () -> {
                await(start);
                try {
                  transactionManager.commit(transaction.getId());
                  return true;
                } catch (TransactionException exception) {
                  return false;
                }
              });

      start.countDown();

      boolean firstSucceeded = first.get(5, TimeUnit.SECONDS);
      boolean secondSucceeded = second.get(5, TimeUnit.SECONDS);

      assertTrue(firstSucceeded ^ secondSucceeded);
    }

    assertEquals(TransactionState.COMMITTED, transaction.getState());
    assertEquals("value", store.get("key", timestampOracle.peek()).orElseThrow());
    assertTrue(snapshotManager.isEmpty());
  }

  @Test
  void shouldSerializeConcurrentCommitAndRollback() throws Exception {
    Transaction<String, String> transaction = transactionManager.begin();
    transaction.put("key", "value");

    CountDownLatch start = new CountDownLatch(1);

    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      Future<?> commit =
          executor.submit(
              () -> {
                await(start);
                try {
                  transactionManager.commit(transaction.getId());
                } catch (TransactionException ignored) {
                  // Rollback may complete before commit acquires the transaction lock.
                }
              });

      Future<?> rollback =
          executor.submit(
              () -> {
                await(start);
                try {
                  transactionManager.rollback(transaction.getId());
                } catch (TransactionException ignored) {
                  // Commit may complete before rollback acquires the transaction lock.
                }
              });

      start.countDown();

      commit.get(5, TimeUnit.SECONDS);
      rollback.get(5, TimeUnit.SECONDS);
    }

    assertTrue(
        transaction.getState() == TransactionState.COMMITTED
            || transaction.getState() == TransactionState.ABORTED);

    assertTrue(snapshotManager.isEmpty());

    if (transaction.getState() == TransactionState.COMMITTED) {
      assertEquals("value", store.get("key", timestampOracle.peek()).orElseThrow());
    } else {
      assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
    }
  }

  @Test
  void shouldRejectNullKeysAndValues() {
    Transaction<String, String> transaction = transactionManager.begin();

    assertThrows(NullPointerException.class, () -> transaction.put(null, "value"));
    assertThrows(NullPointerException.class, () -> transaction.put("key", null));
    assertThrows(NullPointerException.class, () -> transaction.get(null));
    assertThrows(NullPointerException.class, () -> transaction.delete(null));

    transactionManager.rollback(transaction.getId());
  }

  @Test
  void shouldRejectNullTransactionIds() {
    assertThrows(NullPointerException.class, () -> transactionManager.getTransaction(null));
    assertThrows(NullPointerException.class, () -> transactionManager.commit(null));
    assertThrows(NullPointerException.class, () -> transactionManager.rollback(null));
  }

  private void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting to start concurrent operation");
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Concurrent operation was interrupted", exception);
    }
  }

  @Test
  void shouldScanSnapshotVisibleEntries() throws Exception {
    store.put("user:1", "Alice", timestampOracle.now());
    store.put("user:2", "Bob", timestampOracle.now());
    store.put("user:3", "Charlie", timestampOracle.now());

    Transaction<String, String> transaction = transactionManager.begin();

    assertEquals(
        Map.of(
            "user:1", "Alice",
            "user:2", "Bob",
            "user:3", "Charlie"),
        transaction.scan());

    transactionManager.rollback(transaction.getId());
  }

  @Test
  void shouldIncludePendingWritesInScan() throws Exception {
    store.put("user:1", "Alice", timestampOracle.now());

    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("user:1", "Alicia");
    transaction.put("user:2", "Bob");

    assertEquals(
        Map.of(
            "user:1", "Alicia",
            "user:2", "Bob"),
        transaction.scan());

    transactionManager.rollback(transaction.getId());
  }

  @Test
  void shouldExcludePendingDeletesFromScan() throws Exception {
    store.put("user:1", "Alice", timestampOracle.now());
    store.put("user:2", "Bob", timestampOracle.now());
    store.put("user:3", "Charlie", timestampOracle.now());

    Transaction<String, String> transaction = transactionManager.begin();

    transaction.delete("user:2");

    assertEquals(
        Map.of(
            "user:1", "Alice",
            "user:3", "Charlie"),
        transaction.scan());

    transactionManager.rollback(transaction.getId());
  }

  @Test
  void shouldUseLatestPendingMutationDuringScan() throws Exception {
    store.put("user:1", "Alice", timestampOracle.now());

    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("user:1", "Alicia");
    transaction.put("user:1", "Alex");

    assertEquals(Map.of("user:1", "Alex"), transaction.scan());

    transactionManager.rollback(transaction.getId());
  }

  @Test
  void shouldRejectScanAfterCommit() {
    Transaction<String, String> transaction = transactionManager.begin();

    transactionManager.commit(transaction.getId());

    assertThrows(TransactionException.class, transaction::scan);
  }

  @Test
  void shouldReturnImmutableScanResult() throws Exception {
    Transaction<String, String> transaction = transactionManager.begin();

    transaction.put("user:1", "Alice");

    Map<String, String> result = transaction.scan();

    assertThrows(UnsupportedOperationException.class, () -> result.put("user:2", "Bob"));

    transactionManager.rollback(transaction.getId());
  }
}
