package io.astradb.querycoordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.querycoordinator.exception.QueryCoordinatorException;
import io.astradb.querycoordinator.model.QueryExecutionResponse;
import io.astradb.querycoordinator.model.QueryPlan;
import io.astradb.queryexecutor.execution.QueryExecutor;
import io.astradb.queryexecutor.model.ExecutionRequest;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.rocksdb.StringKeySerializer;
import io.astradb.storage.rocksdb.StringValueSerializer;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import io.astradb.transaction.TransactionManager;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for query coordination. */
class QueryCoordinatorTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, String> store;
  private HybridTimestampOracle timestampOracle;
  private SnapshotManager snapshotManager;
  private QueryCoordinator<String, String> queryCoordinator;

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

    TransactionManager<String, String> transactionManager =
        new TransactionManager<>(store, timestampOracle, snapshotManager);

    QueryExecutor<String, String> queryExecutor = new QueryExecutor<>(transactionManager);

    queryCoordinator = new QueryCoordinator<>(queryExecutor);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  @Test
  void shouldExecuteBeginPlanAndReturnTransactionId() {
    QueryPlan<String, String> plan = QueryPlan.create(ExecutionRequest.<String, String>begin());

    QueryExecutionResponse<String> response = queryCoordinator.execute(plan);

    assertNotNull(response.getRequestId());
    assertEquals(plan.getRequestId(), response.getRequestId());
    assertNotNull(response.getExecutionResult().getTransactionId());
    assertEquals(1, snapshotManager.size());
  }

  @Test
  void shouldExecutePutAndGetPlansWithinTransaction() {
    var beginPlan = QueryPlan.create(ExecutionRequest.<String, String>begin());
    var beginResponse = queryCoordinator.execute(beginPlan);
    var transactionId = beginResponse.getExecutionResult().getTransactionId();

    var putPlan = QueryPlan.create(ExecutionRequest.put(transactionId, "key", "value"));
    var putResponse = queryCoordinator.execute(putPlan);

    assertEquals(putPlan.getRequestId(), putResponse.getRequestId());

    var getPlan = QueryPlan.create(ExecutionRequest.<String, String>get(transactionId, "key"));
    var getResponse = queryCoordinator.execute(getPlan);

    assertEquals("value", getResponse.getExecutionResult().getValue().orElseThrow());

    queryCoordinator.execute(QueryPlan.create(ExecutionRequest.commit(transactionId)));
  }

  @Test
  void shouldCommitCoordinatedTransaction() throws Exception {
    var beginResponse =
        queryCoordinator.execute(QueryPlan.create(ExecutionRequest.<String, String>begin()));

    var transactionId = beginResponse.getExecutionResult().getTransactionId();

    queryCoordinator.execute(
        QueryPlan.create(ExecutionRequest.put(transactionId, "key", "committed-value")));

    var commitResponse =
        queryCoordinator.execute(QueryPlan.create(ExecutionRequest.commit(transactionId)));

    assertNotNull(commitResponse.getExecutionResult());
    assertEquals("committed-value", store.get("key", timestampOracle.peek()).orElseThrow());
    assertTrue(snapshotManager.isEmpty());
  }

  @Test
  void shouldRollbackCoordinatedTransaction() throws Exception {
    var beginResponse =
        queryCoordinator.execute(QueryPlan.create(ExecutionRequest.<String, String>begin()));

    var transactionId = beginResponse.getExecutionResult().getTransactionId();

    queryCoordinator.execute(
        QueryPlan.create(ExecutionRequest.put(transactionId, "key", "temporary-value")));

    queryCoordinator.execute(QueryPlan.create(ExecutionRequest.rollback(transactionId)));

    assertTrue(store.get("key", timestampOracle.peek()).isEmpty());
    assertTrue(snapshotManager.isEmpty());
  }

  @Test
  void shouldReturnEmptyValueForMissingKey() {
    var beginResponse =
        queryCoordinator.execute(QueryPlan.create(ExecutionRequest.<String, String>begin()));

    var transactionId = beginResponse.getExecutionResult().getTransactionId();

    var response =
        queryCoordinator.execute(
            QueryPlan.create(ExecutionRequest.get(transactionId, "missing-key")));

    assertTrue(response.getExecutionResult().getValue().isEmpty());

    queryCoordinator.execute(QueryPlan.create(ExecutionRequest.rollback(transactionId)));
  }

  @Test
  void shouldWrapExecutionFailuresInCoordinatorException() {
    var unknownTransactionId = io.astradb.storage.mvcc.TransactionId.generate();

    var plan = QueryPlan.create(ExecutionRequest.<String, String>get(unknownTransactionId, "key"));

    assertThrows(QueryCoordinatorException.class, () -> queryCoordinator.execute(plan));
  }

  @Test
  void shouldRejectNullQueryPlan() {
    assertThrows(NullPointerException.class, () -> queryCoordinator.execute(null));
  }
}
