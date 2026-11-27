package io.astradb.queryexecutor.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.model.SqlExecutionResult;
import io.astradb.queryexecutor.storage.RelationalRowSerializer;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlColumn;
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
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqlExecutionCoordinatorTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, RelationalRow> store;

  private TransactionManager<String, RelationalRow> transactionManager;

  private TransactionBackedRelationalTableAccess tableAccess;

  private Transaction<String, RelationalRow> activeTransaction;

  private SqlExecutionCoordinator coordinator;

  private final SqlPlanner sqlPlanner = new SqlPlanner();

  private final SqlTable usersTable =
      new SqlTable(
          "users",
          List.of(new SqlColumn("id", "INTEGER", false), new SqlColumn("name", "VARCHAR", false)));

  private final SqlSchema schema = new SqlSchema(List.of(usersTable));

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

    coordinator = new SqlExecutionCoordinator(sqlPlanner, tableAccess);

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

  @Test
  void shouldInsertAndSelectWithinSameTransaction() {
    SqlExecutionResult insertResult =
        coordinator.execute(
            "INSERT INTO users (id, name) VALUES (1, 'Yash')", schema, activeTransaction);

    SqlExecutionResult selectResult =
        coordinator.execute("SELECT id, name FROM users WHERE id = 1", schema, activeTransaction);

    assertEquals(1, insertResult.affectedRows());
    assertEquals(0, insertResult.rows().size());

    assertEquals(1, selectResult.rows().size());
    assertEquals(1, selectResult.rows().get(0).getValue(0));
    assertEquals("Yash", selectResult.rows().get(0).getValue(1));
  }

  @Test
  void shouldExecuteMultipleInsertsAndSelectAllRows() {
    coordinator.execute(
        "INSERT INTO users (id, name) VALUES " + "(1, 'Yash'), (2, 'Alex'), (3, 'John')",
        schema,
        activeTransaction);

    coordinator.execute(
        "INSERT INTO users (id, name) VALUES (4, 'Sam')", schema, activeTransaction);

    SqlExecutionResult result =
        coordinator.execute("SELECT name FROM users", schema, activeTransaction);

    assertEquals(4, result.rows().size());

    Set<Object> names =
        result.rows().stream().map(row -> row.getValue(0)).collect(Collectors.toSet());

    assertEquals(Set.of("Yash", "Alex", "John", "Sam"), names);
  }

  @Test
  void shouldPersistInsertedRowsAfterCommit() {
    coordinator.execute(
        "INSERT INTO users (id, name) VALUES (1, 'Yash')", schema, activeTransaction);

    transactionManager.commit(activeTransaction.getId());
    activeTransaction = null;

    Transaction<String, RelationalRow> readTransaction = transactionManager.begin();

    try {
      SqlExecutionResult result =
          coordinator.execute("SELECT id, name FROM users", schema, readTransaction);

      assertEquals(1, result.rows().size());
      assertEquals(1, result.rows().get(0).getValue(0));
      assertEquals("Yash", result.rows().get(0).getValue(1));
    } finally {
      transactionManager.rollback(readTransaction.getId());
    }
  }

  @Test
  void shouldNotExposeInsertedRowsAfterRollback() {
    coordinator.execute(
        "INSERT INTO users (id, name) VALUES (1, 'Yash')", schema, activeTransaction);

    transactionManager.rollback(activeTransaction.getId());
    activeTransaction = null;

    Transaction<String, RelationalRow> readTransaction = transactionManager.begin();

    try {
      SqlExecutionResult result =
          coordinator.execute("SELECT id, name FROM users", schema, readTransaction);

      assertEquals(0, result.rows().size());
    } finally {
      transactionManager.rollback(readTransaction.getId());
    }
  }

  @Test
  void shouldRejectUnsupportedSqlStatement() {
    assertThrows(
        QueryExecutionException.class,
        () -> coordinator.execute("DELETE FROM users WHERE id = 1", schema, activeTransaction));
  }

  @Test
  void shouldUpdateAndSelectWithinSameTransaction() {
    coordinator.execute(
        "INSERT INTO users (id, name) VALUES (1, 'Yash'), (2, 'Alex')", schema, activeTransaction);

    SqlExecutionResult updateResult =
        coordinator.execute(
            "UPDATE users SET name = 'Updated' WHERE id = 1", schema, activeTransaction);

    SqlExecutionResult selectResult =
        coordinator.execute("SELECT id, name FROM users WHERE id = 1", schema, activeTransaction);

    assertEquals(1, updateResult.affectedRows());
    assertEquals(1, selectResult.rows().size());
    assertEquals(1, selectResult.rows().get(0).getValue(0));
    assertEquals("Updated", selectResult.rows().get(0).getValue(1));
  }

  @Test
  void shouldReturnZeroWhenUpdateMatchesNoRows() {
    coordinator.execute(
        "INSERT INTO users (id, name) VALUES (1, 'Yash')", schema, activeTransaction);

    SqlExecutionResult result =
        coordinator.execute(
            "UPDATE users SET name = 'Updated' WHERE id = 999", schema, activeTransaction);

    assertEquals(0, result.affectedRows());
  }
}
