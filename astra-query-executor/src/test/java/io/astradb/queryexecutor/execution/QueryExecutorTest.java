package io.astradb.queryexecutor.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.model.ExecutionRequest;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.rocksdb.KeySerializer;
import io.astradb.storage.rocksdb.StringKeySerializer;
import io.astradb.storage.rocksdb.StringValueSerializer;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import io.astradb.transaction.TransactionManager;
import io.astradb.transaction.TransactionState;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for query execution. */
class QueryExecutorTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, String> store;
  private WalBackedRocksDbMvccStore<String, String> failureStore;
  private HybridTimestampOracle timestampOracle;
  private SnapshotManager snapshotManager;
  private TransactionManager<String, String> transactionManager;
  private QueryExecutor<String, String> queryExecutor;

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
    queryExecutor = new QueryExecutor<>(transactionManager);
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
  void shouldBeginTransaction() {
    var result = queryExecutor.execute(ExecutionRequest.<String, String>begin());

    assertTrue(result.getTransactionId() != null);
    assertEquals(1, snapshotManager.size());

    assertEquals(
        TransactionState.ACTIVE,
        transactionManager.getTransaction(result.getTransactionId()).getState());
  }

  @Test
  void shouldPutAndReadValueWithinTransaction() {
    var beginResult = queryExecutor.execute(ExecutionRequest.<String, String>begin());
    var transactionId = beginResult.getTransactionId();

    queryExecutor.execute(ExecutionRequest.put(transactionId, "key", "value"));

    var result = queryExecutor.execute(ExecutionRequest.get(transactionId, "key"));

    assertEquals("value", result.getValue().orElseThrow());

    queryExecutor.execute(ExecutionRequest.commit(transactionId));
  }

  @Test
  void shouldReturnEmptyResultForMissingKey() {
    var beginResult = queryExecutor.execute(ExecutionRequest.<String, String>begin());

    var result =
        queryExecutor.execute(ExecutionRequest.get(beginResult.getTransactionId(), "missing-key"));

    assertTrue(result.getValue().isEmpty());

    queryExecutor.execute(ExecutionRequest.rollback(beginResult.getTransactionId()));
  }

  @Test
  void shouldCommitTransactionAndPersistValue() throws Exception {
    var beginResult = queryExecutor.execute(ExecutionRequest.<String, String>begin());
    var transactionId = beginResult.getTransactionId();

    queryExecutor.execute(ExecutionRequest.put(transactionId, "key", "committed-value"));

    queryExecutor.execute(ExecutionRequest.commit(transactionId));

    assertEquals("committed-value", store.get("key", timestampOracle.peek()).orElseThrow());
    assertTrue(snapshotManager.isEmpty());
  }

  @Test
  void shouldRollbackTransactionAndDiscardChanges() throws Exception {
    var beginResult = queryExecutor.execute(ExecutionRequest.<String, String>begin());
    var transactionId = beginResult.getTransactionId();

    queryExecutor.execute(ExecutionRequest.put(transactionId, "key", "temporary-value"));

    queryExecutor.execute(ExecutionRequest.rollback(transactionId));

    assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
    assertTrue(snapshotManager.isEmpty());
  }

  @Test
  void shouldDeleteExistingValueOnCommit() throws Exception {
    store.put("key", "existing-value", timestampOracle.now());

    var beginResult = queryExecutor.execute(ExecutionRequest.<String, String>begin());
    var transactionId = beginResult.getTransactionId();

    queryExecutor.execute(ExecutionRequest.delete(transactionId, "key"));
    queryExecutor.execute(ExecutionRequest.commit(transactionId));

    assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
  }

  @Test
  void shouldRejectOperationsWithUnknownTransactionId() {
    var transactionId = io.astradb.storage.mvcc.TransactionId.generate();

    assertThrows(
        QueryExecutionException.class,
        () -> queryExecutor.execute(ExecutionRequest.get(transactionId, "key")));
  }

  @Test
  void shouldRejectNullExecutionRequest() {
    assertThrows(NullPointerException.class, () -> queryExecutor.execute(null));
  }

  @Test
  void shouldPreserveUncertainCommitOutcome() throws Exception {
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

    TransactionManager<String, String> failingManager =
        new TransactionManager<>(failureStore, timestampOracle, snapshotManager);

    QueryExecutor<String, String> failingExecutor = new QueryExecutor<>(failingManager);

    var beginResult = failingExecutor.execute(ExecutionRequest.<String, String>begin());
    var transactionId = beginResult.getTransactionId();

    failingExecutor.execute(ExecutionRequest.put(transactionId, "key", "value"));

    assertThrows(
        QueryExecutionException.class,
        () -> failingExecutor.execute(ExecutionRequest.commit(transactionId)));

    assertEquals(
        TransactionState.IN_DOUBT, failingManager.getTransaction(transactionId).getState());

    assertEquals(1, snapshotManager.size());
  }
}
