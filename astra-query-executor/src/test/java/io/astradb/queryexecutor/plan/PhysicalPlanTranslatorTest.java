package io.astradb.queryexecutor.plan;

import static org.junit.jupiter.api.Assertions.*;

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
import io.astradb.transaction.TransactionState;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PhysicalPlanTranslatorTest {

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
  void shouldTranslateAndExecuteSelectWithFilterAndProjection() {
    // Arrange: insert and commit test data.
    Transaction<String, RelationalRow> insertTransaction = transactionManager.begin();

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(5, "Alice"));

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(15, "Bob"));

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(25, "Charlie"));

    transactionManager.commit(insertTransaction.getId());

    // Begin a separate transaction for query execution.
    activeTransaction = transactionManager.begin();

    // Generate the Calcite logical plan.
    SqlPlanner planner = new SqlPlanner();

    SqlPlan sqlPlan = planner.plan("SELECT name FROM users WHERE id > 10", schema);

    assertNotNull(sqlPlan.logicalPlan().orElse(null));

    // Translate the logical plan into physical operators.
    PhysicalPlanTranslator translator =
        new PhysicalPlanTranslator(tableAccess, activeTransaction, schema);

    PhysicalOperatorHolder holder =
        new PhysicalOperatorHolder(translator.translate(sqlPlan.logicalPlan().orElseThrow()));

    // Execute the physical operator tree.
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();

    List<RelationalRow> results = executor.execute(holder.operator());

    // Assert: only Bob and Charlie should be returned.
    assertEquals(
        Set.of("Bob", "Charlie"),
        results.stream().map(row -> row.getValue(0)).collect(java.util.stream.Collectors.toSet()));

    assertEquals(2, results.size());
  }

  @Test
  void shouldSortAscendingAndHideInternalSortColumn() {
    Transaction<String, RelationalRow> insertTransaction = transactionManager.begin();

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(25, "Charlie"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(5, "Alice"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(15, "Bob"));

    transactionManager.commit(insertTransaction.getId());

    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = new SqlPlanner().plan("SELECT name FROM users ORDER BY id ASC", schema);

    PhysicalPlanTranslator translator =
        new PhysicalPlanTranslator(tableAccess, activeTransaction, schema);

    List<RelationalRow> results = new PhysicalPlanExecutor().execute(translator.translate(sqlPlan));

    assertEquals(
        List.of("Alice", "Bob", "Charlie"), results.stream().map(row -> row.getValue(0)).toList());

    assertEquals(1, results.get(0).size());
  }

  @Test
  void shouldSortDescendingAndHideInternalSortColumn() {
    Transaction<String, RelationalRow> insertTransaction = transactionManager.begin();

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(25, "Charlie"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(5, "Alice"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(15, "Bob"));

    transactionManager.commit(insertTransaction.getId());

    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = new SqlPlanner().plan("SELECT name FROM users ORDER BY id DESC", schema);

    PhysicalPlanTranslator translator =
        new PhysicalPlanTranslator(tableAccess, activeTransaction, schema);

    List<RelationalRow> results = new PhysicalPlanExecutor().execute(translator.translate(sqlPlan));

    assertEquals(
        List.of("Charlie", "Bob", "Alice"), results.stream().map(row -> row.getValue(0)).toList());

    assertEquals(1, results.get(0).size());
  }

  @Test
  void shouldApplyLimitWithoutOrderBy() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = new SqlPlanner().plan("SELECT name FROM users LIMIT 2", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());
    assertEquals(1, results.get(0).size());
  }

  @Test
  void shouldApplyOffsetWithoutOrderBy() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = new SqlPlanner().plan("SELECT name FROM users OFFSET 2", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(1, results.size());
    assertEquals(1, results.get(0).size());
  }

  @Test
  void shouldApplyLimitAndOffsetTogether() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = new SqlPlanner().plan("SELECT name FROM users LIMIT 2 OFFSET 1", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());
    assertEquals(1, results.get(0).size());
  }

  @Test
  void shouldApplyOrderByLimitAndOffsetTogether() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan =
        new SqlPlanner().plan("SELECT name FROM users ORDER BY id DESC LIMIT 1 OFFSET 1", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(1, results.size());
    assertEquals("Bob", results.get(0).getValue(0));
    assertEquals(1, results.get(0).size());
  }

  private void insertTestUsers() {
    Transaction<String, RelationalRow> insertTransaction = transactionManager.begin();

    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(25, "Charlie"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(5, "Alice"));
    tableAccess.insert(insertTransaction, usersTable, RelationalRow.of(15, "Bob"));

    transactionManager.commit(insertTransaction.getId());
  }

  private List<RelationalRow> execute(SqlPlan sqlPlan) {
    PhysicalPlanTranslator translator =
        new PhysicalPlanTranslator(tableAccess, activeTransaction, schema);

    return new PhysicalPlanExecutor().execute(translator.translate(sqlPlan));
  }

  @Test
  void shouldPreserveAllColumnsForSelectStarWithSortingAndPagination() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan =
        new SqlPlanner().plan("SELECT * FROM users ORDER BY id DESC LIMIT 2 OFFSET 1", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());

    // Expected order: Bob (15), Alice (5).
    assertEquals(15, results.get(0).getValue(0));
    assertEquals("Bob", results.get(0).getValue(1));

    assertEquals(5, results.get(1).getValue(0));
    assertEquals("Alice", results.get(1).getValue(1));

    // SELECT * must preserve both table columns.
    assertEquals(2, results.get(0).size());
  }

  @Test
  void shouldPreserveExplicitProjectionWithSortingAndLimit() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan =
        new SqlPlanner().plan("SELECT id, name FROM users ORDER BY id DESC LIMIT 2", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());
    assertEquals(2, results.get(0).size());

    assertEquals(25, results.get(0).getValue(0));
    assertEquals("Charlie", results.get(0).getValue(1));

    assertEquals(15, results.get(1).getValue(0));
    assertEquals("Bob", results.get(1).getValue(1));
  }

  @Test
  void shouldRemoveHiddenSortColumnAfterPagination() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan =
        new SqlPlanner().plan("SELECT name FROM users ORDER BY id DESC LIMIT 2 OFFSET 1", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());

    assertEquals("Bob", results.get(0).getValue(0));
    assertEquals("Alice", results.get(1).getValue(0));

    // Internal sort column (id) must not appear in the result.
    assertEquals(1, results.get(0).size());
    assertEquals(1, results.get(1).size());
  }

  @Test
  void shouldExecuteSqlAdditionProjection() {
    insertTestUsers();

    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = sqlPlanner.plan("SELECT id + 10 FROM users", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(3, results.size());

    assertTrue(results.stream().anyMatch(row -> new BigDecimal("35").equals(row.getValue(0))));
    assertTrue(results.stream().anyMatch(row -> new BigDecimal("15").equals(row.getValue(0))));
    assertTrue(results.stream().anyMatch(row -> new BigDecimal("25").equals(row.getValue(0))));
  }

  @Test
  void shouldExecuteMultipleComputedProjections() {
    insertTestUsers();

    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = sqlPlanner.plan("SELECT id * 2, name FROM users", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(3, results.size());

    assertTrue(
        results.stream()
            .anyMatch(
                row ->
                    row.getValue(0).equals(new BigDecimal("50"))
                        && row.getValue(1).equals("Charlie")));

    assertTrue(
        results.stream()
            .anyMatch(
                row ->
                    row.getValue(0).equals(new BigDecimal("10"))
                        && row.getValue(1).equals("Alice")));

    assertTrue(
        results.stream()
            .anyMatch(
                row ->
                    row.getValue(0).equals(new BigDecimal("30")) && row.getValue(1).equals("Bob")));
  }

  @Test
  void shouldExecuteArithmeticAdditionInWherePredicate() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = sqlPlanner.plan("SELECT name FROM users WHERE id + 10 > 20", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());
    assertTrue(results.stream().anyMatch(row -> "Charlie".equals(row.getValue(0))));
    assertTrue(results.stream().anyMatch(row -> "Bob".equals(row.getValue(0))));
  }

  @Test
  void shouldExecuteArithmeticMultiplicationInWherePredicate() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = sqlPlanner.plan("SELECT name FROM users WHERE id * 2 >= 30", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());
    assertTrue(results.stream().anyMatch(row -> "Charlie".equals(row.getValue(0))));
    assertTrue(results.stream().anyMatch(row -> "Bob".equals(row.getValue(0))));
  }

  @Test
  void shouldExecuteArithmeticSubtractionInWherePredicate() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = sqlPlanner.plan("SELECT name FROM users WHERE id - 5 = 10", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(1, results.size());
    assertEquals("Bob", results.get(0).getValue(0));
  }

  @Test
  void shouldExecuteArithmeticDivisionInWherePredicate() {
    insertTestUsers();
    activeTransaction = transactionManager.begin();

    SqlPlan sqlPlan = sqlPlanner.plan("SELECT name FROM users WHERE id / 5 >= 3", schema);

    List<RelationalRow> results = execute(sqlPlan);

    assertEquals(2, results.size());
    assertTrue(results.stream().anyMatch(row -> "Charlie".equals(row.getValue(0))));
    assertTrue(results.stream().anyMatch(row -> "Bob".equals(row.getValue(0))));
  }

  private record PhysicalOperatorHolder(
      io.astradb.queryexecutor.operator.PhysicalOperator operator) {}
}
