package io.astradb.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.rocksdb.StringKeySerializer;
import io.astradb.storage.rocksdb.StringValueSerializer;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for transaction lifecycle management. */
class TransactionManagerTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, String> store;
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
}
