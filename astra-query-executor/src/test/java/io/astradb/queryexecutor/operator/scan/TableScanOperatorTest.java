package io.astradb.queryexecutor.operator.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.queryexecutor.operator.TableScanOperator;
import io.astradb.queryexecutor.storage.RelationalRowSerializer;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlTable;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.rocksdb.StringKeySerializer;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import io.astradb.transaction.Transaction;
import io.astradb.transaction.TransactionManager;
import io.astradb.transaction.TransactionState;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableScanOperatorTest {

  @TempDir Path tempDir;

  private WalBackedRocksDbMvccStore<String, RelationalRow> store;
  private TransactionManager<String, RelationalRow> transactionManager;
  private TransactionBackedRelationalTableAccess tableAccess;
  private Transaction<String, RelationalRow> activeTransaction;

  private final SqlTable usersTable =
      new SqlTable(
          "users",
          List.of(new SqlColumn("id", "INTEGER", false), new SqlColumn("name", "VARCHAR", false)));

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
  void shouldScanCommittedRowsFromTable() {
    Transaction<String, RelationalRow> insertTransaction = transactionManager.begin();

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(1, "Alice"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(2, "Bob"));

    transactionManager.commit(insertTransaction.getId());

    activeTransaction = transactionManager.begin();

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    List<RelationalRow> rows = collectRows(operator);

    assertEquals(
        Set.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob")), new HashSet<>(rows));

    assertEquals(2, rows.size());
  }

  @Test
  void shouldIncludeUncommittedInserts() {
    activeTransaction = transactionManager.begin();

    tableAccess.insert(activeTransaction, usersTable, RelationalRow.of(1, "Alice"));
    tableAccess.insert(activeTransaction, usersTable, RelationalRow.of(2, "Bob"));

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    List<RelationalRow> rows = collectRows(operator);

    assertEquals(2, rows.size());
    assertEquals(
        Set.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob")), new HashSet<>(rows));
  }

  @Test
  void shouldExcludePendingDeletes() {
    activeTransaction = transactionManager.begin();

    String deletedRowKey =
        tableAccess.insert(activeTransaction, usersTable, RelationalRow.of(1, "Alice"));

    tableAccess.insert(activeTransaction, usersTable, RelationalRow.of(2, "Bob"));

    activeTransaction.delete(deletedRowKey);

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    List<RelationalRow> rows = collectRows(operator);

    assertEquals(List.of(RelationalRow.of(2, "Bob")), rows);
  }

  @Test
  void shouldReturnEmptyResultForEmptyTable() {
    activeTransaction = transactionManager.begin();

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    try (RowIterator iterator = operator.open()) {
      assertFalse(iterator.hasNext());
    }

    operator.close();
  }

  @Test
  void shouldRejectOpeningOperatorMoreThanOnce() {
    activeTransaction = transactionManager.begin();

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  @Test
  void shouldCloseIteratorAndStopIteration() {
    activeTransaction = transactionManager.begin();

    tableAccess.insert(activeTransaction, usersTable, RelationalRow.of(1, "Alice"));

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    RowIterator iterator = operator.open();

    assertTrue(iterator.hasNext());

    iterator.close();

    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);

    operator.close();
  }

  @Test
  void shouldCloseActiveIteratorWhenOperatorCloses() {
    activeTransaction = transactionManager.begin();

    tableAccess.insert(activeTransaction, usersTable, RelationalRow.of(1, "Alice"));

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    RowIterator iterator = operator.open();

    operator.close();

    assertFalse(iterator.hasNext());
  }

  @Test
  void shouldRejectOpeningOperatorAfterClose() {
    activeTransaction = transactionManager.begin();

    TableScanOperator operator = new TableScanOperator(tableAccess, activeTransaction, usersTable);

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  private List<RelationalRow> collectRows(TableScanOperator operator) {
    try (RowIterator iterator = operator.open()) {
      java.util.ArrayList<RelationalRow> rows = new java.util.ArrayList<>();

      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }

      return List.copyOf(rows);
    } finally {
      operator.close();
    }
  }
}
