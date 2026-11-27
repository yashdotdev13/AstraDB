package io.astradb.queryexecutor.dml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.storage.RelationalRowSerializer;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import io.astradb.sql.planning.SqlPlanner;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.rocksdb.StringKeySerializer;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import io.astradb.transaction.Transaction;
import io.astradb.transaction.TransactionManager;
import io.astradb.transaction.TransactionState;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MergeExecutorTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, RelationalRow> store;
  private TransactionManager<String, RelationalRow> transactionManager;
  private TransactionBackedRelationalTableAccess tableAccess;
  private Transaction<String, RelationalRow> activeTransaction;

  private final SqlPlanner sqlPlanner = new SqlPlanner();

  private final SqlTable usersTable =
      new SqlTable(
          "users",
          List.of(new SqlColumn("id", "INTEGER", false), new SqlColumn("name", "VARCHAR", false)));

  private final SqlTable incomingUsersTable =
      new SqlTable(
          "incoming_users",
          List.of(new SqlColumn("id", "INTEGER", false), new SqlColumn("name", "VARCHAR", true)));

  private final SqlSchema schema = new SqlSchema(List.of(usersTable, incomingUsersTable));

  private DmlExecutor dmlExecutor;

  @BeforeEach
  void setUp() throws Exception {
    store =
        WalBackedRocksDbMvccStore.open(
            tempDir.resolve("rocksdb"),
            tempDir.resolve("store.wal"),
            new StringKeySerializer(),
            new RelationalRowSerializer());

    transactionManager =
        new TransactionManager<>(store, new HybridTimestampOracle(), new SnapshotManager());

    tableAccess = new TransactionBackedRelationalTableAccess();
    dmlExecutor = new DmlExecutor(tableAccess);
    activeTransaction = transactionManager.begin();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (activeTransaction != null && activeTransaction.getState() == TransactionState.ACTIVE) {
      transactionManager.rollback(activeTransaction.getId());
    }

    if (store != null) {
      store.close();
    }
  }

  private void executeInsert(String sql) {
    SqlPlan plan = sqlPlanner.plan(sql, schema);
    dmlExecutor.execute(plan, schema, activeTransaction);
  }

  private DmlExecutionResult executeMerge() {
    String sql =
        """
                MERGE INTO users AS target
                USING incoming_users AS source
                ON target.id = source.id
                WHEN MATCHED THEN
                    UPDATE SET name = source.name
                WHEN NOT MATCHED THEN
                    INSERT (id, name)
                    VALUES (source.id, source.name)
                """;

    SqlPlan plan = sqlPlanner.plan(sql, schema);
    return dmlExecutor.execute(plan, schema, activeTransaction);
  }

  @Test
  void shouldUpdateMatchedRows() {
    executeInsert("INSERT INTO users (id, name) VALUES (1, 'Old')");
    executeInsert("INSERT INTO incoming_users (id, name) VALUES (1, 'Updated')");

    DmlExecutionResult result = executeMerge();

    assertEquals(1, result.affectedRows());

    RelationalRow row = tableAccess.scan(activeTransaction, usersTable).get(0);

    assertEquals(1, row.getValue(0));
    assertEquals("Updated", row.getValue(1));
  }

  @Test
  void shouldInsertUnmatchedRows() {
    executeInsert("INSERT INTO users (id, name) VALUES (1, 'Existing')");
    executeInsert("INSERT INTO incoming_users (id, name) VALUES (2, 'New')");

    DmlExecutionResult result = executeMerge();

    assertEquals(1, result.affectedRows());
    assertEquals(2, tableAccess.scan(activeTransaction, usersTable).size());
  }

  @Test
  void shouldUpdateAndInsertInSameMerge() {
    executeInsert("INSERT INTO users (id, name) VALUES (1, 'Old')");
    executeInsert("INSERT INTO incoming_users (id, name) VALUES (1, 'Updated'), (2, 'Inserted')");

    DmlExecutionResult result = executeMerge();

    assertEquals(2, result.affectedRows());
    assertEquals(2, tableAccess.scan(activeTransaction, usersTable).size());

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, usersTable);

    assertEquals(
        "Updated",
        rows.stream()
            .filter(row -> row.getValue(0).equals(1))
            .findFirst()
            .orElseThrow()
            .getValue(1));

    assertEquals(
        "Inserted",
        rows.stream()
            .filter(row -> row.getValue(0).equals(2))
            .findFirst()
            .orElseThrow()
            .getValue(1));
  }

  @Test
  void shouldReturnZeroWhenSourceIsEmpty() {
    executeInsert("INSERT INTO users (id, name) VALUES (1, 'Existing')");

    DmlExecutionResult result = executeMerge();

    assertEquals(0, result.affectedRows());
    assertEquals(1, tableAccess.scan(activeTransaction, usersTable).size());
  }

  @Test
  void shouldNotStagePartialChangesWhenInsertValidationFails() {
    executeInsert("INSERT INTO users (id, name) VALUES (1, 'Original')");
    executeInsert("INSERT INTO incoming_users (id, name) VALUES (1, 'Updated'), (2, NULL)");

    assertThrows(QueryExecutionException.class, this::executeMerge);

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, usersTable);

    assertEquals(1, rows.size());
    assertEquals("Original", rows.get(0).getValue(1));
  }

  @Test
  void shouldRollbackMergedChanges() {
    executeInsert("INSERT INTO users (id, name) VALUES (1, 'Original')");
    executeInsert("INSERT INTO incoming_users (id, name) VALUES (1, 'Updated'), (2, 'New')");

    transactionManager.commit(activeTransaction.getId());
    activeTransaction = transactionManager.begin();

    executeMerge();

    transactionManager.rollback(activeTransaction.getId());
    activeTransaction = null;

    Transaction<String, RelationalRow> readTransaction = transactionManager.begin();

    try {
      List<RelationalRow> rows = tableAccess.scan(readTransaction, usersTable);

      assertEquals(1, rows.size());
      assertEquals("Original", rows.get(0).getValue(1));
    } finally {
      transactionManager.rollback(readTransaction.getId());
    }
  }
}
