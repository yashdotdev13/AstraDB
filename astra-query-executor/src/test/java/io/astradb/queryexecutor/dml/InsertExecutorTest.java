package io.astradb.queryexecutor.dml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.storage.RelationalRowSerializer;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.exception.SqlValidationException;
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
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InsertExecutorTest {

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

  private final SqlTable profilesTable =
      new SqlTable(
          "profiles",
          List.of(
              new SqlColumn("id", "INTEGER", false), new SqlColumn("nickname", "VARCHAR", true)));

  private final SqlSchema schema = new SqlSchema(List.of(usersTable, profilesTable));

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
    if (activeTransaction != null
        && activeTransaction.getState() == io.astradb.transaction.TransactionState.ACTIVE) {
      transactionManager.rollback(activeTransaction.getId());
    }

    if (store != null) {
      store.close();
    }
  }

  @Test
  void shouldInsertSingleRow() {
    SqlPlan plan = sqlPlanner.plan("INSERT INTO users (id, name) VALUES (1, 'Yash')", schema);

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, activeTransaction);

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, usersTable);

    assertEquals(1, result.affectedRows());
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getValue(0));
    assertEquals("Yash", rows.get(0).getValue(1));
  }

  @Test
  void shouldInsertMultipleRows() {
    SqlPlan plan =
        sqlPlanner.plan(
            "INSERT INTO users (id, name) VALUES " + "(1, 'Yash'), (2, 'Alex'), (3, 'John')",
            schema);

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, activeTransaction);

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, usersTable);

    assertEquals(3, result.affectedRows());
    assertEquals(3, rows.size());
  }

  @Test
  void shouldMapValuesUsingExplicitColumnOrder() {
    SqlPlan plan = sqlPlanner.plan("INSERT INTO users (name, id) VALUES ('Yash', 10)", schema);

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, activeTransaction);

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, usersTable);

    assertEquals(1, result.affectedRows());
    assertEquals(10, rows.get(0).getValue(0));
    assertEquals("Yash", rows.get(0).getValue(1));
  }

  @Test
  void shouldRejectNullForNonNullableColumn() {
    assertThrows(
        SqlValidationException.class,
        () -> sqlPlanner.plan("INSERT INTO users (id, name) VALUES (1, NULL)", schema));

    assertEquals(0, tableAccess.scan(activeTransaction, usersTable).size());
  }

  @Test
  void shouldNotStageEarlierRowsWhenLaterRowIsInvalid() {
    SqlPlan plan =
        sqlPlanner.plan(
            "INSERT INTO users (id, name) VALUES " + "(1, 'Yash'), (2.5, 'Alex')", schema);

    assertThrows(
        QueryExecutionException.class, () -> dmlExecutor.execute(plan, schema, activeTransaction));

    assertEquals(0, tableAccess.scan(activeTransaction, usersTable).size());
  }

  @Test
  void shouldRollbackInsertedRows() {
    SqlPlan plan = sqlPlanner.plan("INSERT INTO users (id, name) VALUES (1, 'Yash')", schema);

    dmlExecutor.execute(plan, schema, activeTransaction);

    transactionManager.rollback(activeTransaction.getId());
    activeTransaction = null;

    Transaction<String, RelationalRow> readTransaction = transactionManager.begin();

    try {
      assertEquals(0, tableAccess.scan(readTransaction, usersTable).size());
    } finally {
      transactionManager.rollback(readTransaction.getId());
    }
  }

  @Test
  void shouldInsertNullIntoNullableColumn() {
    SqlPlan plan = sqlPlanner.plan("INSERT INTO profiles (id, nickname) VALUES (1, NULL)", schema);

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, activeTransaction);

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, profilesTable);

    assertEquals(1, result.affectedRows());
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getValue(0));
    assertEquals(null, rows.get(0).getValue(1));
  }

  @Test
  void shouldAllowOmittingNullableColumn() {
    SqlPlan plan = sqlPlanner.plan("INSERT INTO profiles (id) VALUES (2)", schema);

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, activeTransaction);

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, profilesTable);

    assertEquals(1, result.affectedRows());
    assertEquals(2, rows.get(0).getValue(0));
    assertEquals(null, rows.get(0).getValue(1));
  }
}
