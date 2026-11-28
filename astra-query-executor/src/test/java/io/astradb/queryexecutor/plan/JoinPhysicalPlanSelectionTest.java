package io.astradb.queryexecutor.plan;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.astradb.common.time.HybridTimestampOracle;
import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.join.HashJoinOperator;
import io.astradb.queryexecutor.operator.join.NestedLoopJoinOperator;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JoinPhysicalPlanSelectionTest {

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

  private final SqlTable ordersTable =
      new SqlTable(
          "orders",
          List.of(
              new SqlColumn("order_id", "INTEGER", false),
              new SqlColumn("user_id", "INTEGER", false),
              new SqlColumn("amount", "DECIMAL", false)));

  private final SqlTable tenantUsersTable =
      new SqlTable(
          "tenant_users",
          List.of(
              new SqlColumn("tenant_id", "INTEGER", false),
              new SqlColumn("user_id", "INTEGER", false),
              new SqlColumn("name", "VARCHAR", false)));

  private final SqlTable tenantOrdersTable =
      new SqlTable(
          "tenant_orders",
          List.of(
              new SqlColumn("tenant_id", "INTEGER", false),
              new SqlColumn("user_id", "INTEGER", false),
              new SqlColumn("order_id", "INTEGER", false)));

  private final SqlSchema schema =
      new SqlSchema(List.of(usersTable, ordersTable, tenantUsersTable, tenantOrdersTable));

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
  void shouldSelectHashJoinForSingleKeyInnerJoin() {
    PhysicalOperator operator =
        translateJoin(
            """
                        SELECT u.id
                        FROM users u
                        INNER JOIN orders o ON u.id = o.user_id
                        """);

    try {
      assertInstanceOf(HashJoinOperator.class, operator);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldSelectHashJoinForCompositeKeyInnerJoin() {
    PhysicalOperator operator =
        translateJoin(
            """
                        SELECT u.user_id
                        FROM tenant_users u
                        INNER JOIN tenant_orders o
                            ON u.tenant_id = o.tenant_id
                            AND u.user_id = o.user_id
                        """);

    try {
      assertInstanceOf(HashJoinOperator.class, operator);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldSelectNestedLoopJoinForLeftJoin() {
    PhysicalOperator operator =
        translateJoin(
            """
                        SELECT u.id
                        FROM users u
                        LEFT JOIN orders o ON u.id = o.user_id
                        """);

    try {
      assertInstanceOf(NestedLoopJoinOperator.class, operator);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldSelectNestedLoopJoinForNonEquiInnerJoin() {
    PhysicalOperator operator =
        translateJoin(
            """
                        SELECT u.id
                        FROM users u
                        INNER JOIN orders o ON u.id > o.user_id
                        """);

    try {
      assertInstanceOf(NestedLoopJoinOperator.class, operator);
    } finally {
      operator.close();
    }
  }

  /**
   * Plans the SQL query, extracts its logical Join node, and translates that node directly.
   *
   * <p>Translating the Join node directly allows these tests to inspect the selected physical join
   * operator without traversing projection operators above it.
   */
  private PhysicalOperator translateJoin(String sql) {
    SqlPlan sqlPlan = sqlPlanner.plan(sql, schema);

    RelNode logicalPlan = sqlPlan.logicalPlan().orElseThrow();

    Join join = findJoin(logicalPlan);

    assertNotNull(join, "SQL logical plan must contain a Join node");

    PhysicalPlanTranslator translator =
        new PhysicalPlanTranslator(tableAccess, activeTransaction, schema);

    return translator.translate(join);
  }

  /**
   * Recursively finds a Join node in a Calcite relational plan.
   *
   * @param node relational plan node
   * @return first Join node found
   */
  private Join findJoin(RelNode node) {
    if (node instanceof Join join) {
      return join;
    }

    for (RelNode input : node.getInputs()) {
      Join join = findJoin(input);

      if (join != null) {
        return join;
      }
    }

    return null;
  }
}
