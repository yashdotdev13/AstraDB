package io.astradb.queryexecutor.dml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.astradb.common.time.HybridTimestampOracle;
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
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UpdateExecutorTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, RelationalRow> store;
  private TransactionManager<String, RelationalRow> transactionManager;
  private TransactionBackedRelationalTableAccess tableAccess;
  private Transaction<String, RelationalRow> activeTransaction;

  private final SqlPlanner sqlPlanner = new SqlPlanner();

  private final SqlTable usersTable =
      new SqlTable(
          "users",
          List.of(
              new SqlColumn("id", "INTEGER", false),
              new SqlColumn("name", "VARCHAR", false),
              new SqlColumn("age", "INTEGER", false),
              new SqlColumn("nickname", "VARCHAR", true)));

  private final SqlSchema schema = new SqlSchema(List.of(usersTable));

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

  private void insertUsers() {
    SqlPlan plan =
        sqlPlanner.plan(
            "INSERT INTO users (id, name, age, nickname) VALUES "
                + "(1, 'Yash', 21, 'YC'), "
                + "(2, 'Alex', 25, 'AX'), "
                + "(3, 'John', 30, NULL)",
            schema);

    dmlExecutor.execute(plan, schema, activeTransaction);
  }

  private DmlExecutionResult executeUpdate(String sql) {
    SqlPlan plan = sqlPlanner.plan(sql, schema);
    return dmlExecutor.execute(plan, schema, activeTransaction);
  }

  @Test
  void shouldUpdateMatchingRow() {
    insertUsers();

    DmlExecutionResult result = executeUpdate("UPDATE users SET name = 'Updated' WHERE id = 1");

    assertEquals(1, result.affectedRows());

    RelationalRow row =
        tableAccess.scan(activeTransaction, usersTable).stream()
            .filter(value -> value.getValue(0).equals(1))
            .findFirst()
            .orElseThrow();

    assertEquals("Updated", row.getValue(1));
    assertEquals(21, row.getValue(2));
  }

  @Test
  void shouldUpdateMultipleMatchingRows() {
    insertUsers();

    DmlExecutionResult result =
        executeUpdate("UPDATE users SET nickname = 'Member' WHERE age >= 25");

    assertEquals(2, result.affectedRows());

    List<RelationalRow> rows = tableAccess.scan(activeTransaction, usersTable);

    assertEquals(2, rows.stream().filter(row -> "Member".equals(row.getValue(3))).count());
  }

  @Test
  void shouldUpdateAllRowsWhenWhereClauseIsAbsent() {
    insertUsers();

    DmlExecutionResult result = executeUpdate("UPDATE users SET name = 'Updated'");

    assertEquals(3, result.affectedRows());

    assertEquals(
        3,
        tableAccess.scan(activeTransaction, usersTable).stream()
            .filter(row -> "Updated".equals(row.getValue(1)))
            .count());
  }

  @Test
  void shouldEvaluateArithmeticUpdateExpression() {
    insertUsers();

    DmlExecutionResult result = executeUpdate("UPDATE users SET age = age + 1 WHERE id = 1");

    assertEquals(1, result.affectedRows());

    RelationalRow row =
        tableAccess.scan(activeTransaction, usersTable).stream()
            .filter(value -> value.getValue(0).equals(1))
            .findFirst()
            .orElseThrow();

    assertEquals(22, row.getValue(2));
  }

  @Test
  void shouldReturnZeroWhenNoRowsMatch() {
    insertUsers();

    DmlExecutionResult result = executeUpdate("UPDATE users SET name = 'Updated' WHERE id = 999");

    assertEquals(0, result.affectedRows());
    assertEquals(3, tableAccess.scan(activeTransaction, usersTable).size());
  }

  @Test
  void shouldUpdateNullableColumnToNull() {
    insertUsers();

    DmlExecutionResult result = executeUpdate("UPDATE users SET nickname = NULL WHERE id = 1");

    assertEquals(1, result.affectedRows());

    RelationalRow row =
        tableAccess.scan(activeTransaction, usersTable).stream()
            .filter(value -> value.getValue(0).equals(1))
            .findFirst()
            .orElseThrow();

    assertEquals(null, row.getValue(3));
  }

  @Test
  void shouldRollbackUpdatedRows() {
    insertUsers();

    transactionManager.commit(activeTransaction.getId());
    activeTransaction = transactionManager.begin();

    executeUpdate("UPDATE users SET name = 'Updated' WHERE id = 1");

    transactionManager.rollback(activeTransaction.getId());
    activeTransaction = null;

    Transaction<String, RelationalRow> readTransaction = transactionManager.begin();

    try {
      RelationalRow row =
          tableAccess.scan(readTransaction, usersTable).stream()
              .filter(value -> value.getValue(0).equals(1))
              .findFirst()
              .orElseThrow();

      assertEquals("Yash", row.getValue(1));
    } finally {
      transactionManager.rollback(readTransaction.getId());
    }
  }
}
