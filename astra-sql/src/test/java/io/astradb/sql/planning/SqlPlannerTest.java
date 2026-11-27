package io.astradb.sql.planning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.sql.exception.SqlValidationException;
import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlStatementType;
import io.astradb.sql.model.SqlTable;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqlPlannerTest {

  private SqlPlanner planner;
  private SqlSchema schema;

  @BeforeEach
  void setUp() {
    planner = new SqlPlanner();

    schema =
        new SqlSchema(
            List.of(
                new SqlTable(
                    "users",
                    List.of(
                        new SqlColumn("id", "INTEGER", false),
                        new SqlColumn("name", "VARCHAR", true)))));
  }

  @Test
  void shouldCreateLogicalPlanForSelectStatement() {
    String sql = "SELECT id, name FROM users";

    SqlPlan plan = planner.plan(sql, schema);

    assertNotNull(plan);
    assertEquals(sql, plan.sql());
    assertEquals(SqlStatementType.SELECT, plan.statementType());
    assertNotNull(plan.validatedStatement());

    assertTrue(plan.logicalPlan().isPresent());
    assertTrue(plan.logicalPlan().get() instanceof RelNode);
  }

  @Test
  void shouldCreatePlanForInsertStatement() {
    String sql = "INSERT INTO users (id, name) VALUES (1, 'Yash')";

    SqlPlan plan = planner.plan(sql, schema);

    assertEquals(SqlStatementType.INSERT, plan.statementType());
    assertEquals(sql, plan.sql());
    assertTrue(plan.logicalPlan().isEmpty());
  }

  @Test
  void shouldInspectUpdateLogicalPlan() {
    SqlPlan plan = planner.plan("UPDATE users SET name = 'Updated' WHERE id = 1", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    System.out.println("\n===== UPDATE LOGICAL PLAN =====");
    System.out.println(org.apache.calcite.plan.RelOptUtil.toString(logicalPlan));

    System.out.println("\n===== UPDATE NODE =====");
    System.out.println(logicalPlan.getClass().getName());

    TableModify modify = (TableModify) logicalPlan;

    System.out.println("Operation: " + modify.getOperation());
    System.out.println("Update columns: " + modify.getUpdateColumnList());
    System.out.println("Source expressions: " + modify.getSourceExpressionList());

    System.out.println("\n===== INPUT NODE =====");
    System.out.println(modify.getInput().getClass().getName());
    System.out.println(org.apache.calcite.plan.RelOptUtil.toString(modify.getInput()));
  }

  @Test
  void shouldCreateLogicalTableModifyPlanForDeleteStatement() {
    String sql = "DELETE FROM users WHERE id = 1";

    SqlPlan plan = planner.plan(sql, schema);
    assertEquals(SqlStatementType.DELETE, plan.statementType());
    RelNode logicalPlan = plan.logicalPlan().orElseThrow();
    assertTrue(logicalPlan instanceof TableModify);
    TableModify tableModify = (TableModify) logicalPlan;
    assertEquals(TableModify.Operation.DELETE, tableModify.getOperation());
  }

  @Test
  void shouldRejectStatementWithUnknownTable() {
    String sql = "SELECT id FROM unknown_table";

    assertThrows(SqlValidationException.class, () -> planner.plan(sql, schema));
  }

  @Test
  void shouldRejectStatementWithUnknownColumn() {
    String sql = "SELECT email FROM users";

    assertThrows(SqlValidationException.class, () -> planner.plan(sql, schema));
  }

  @Test
  void shouldRejectNullSql() {
    assertThrows(NullPointerException.class, () -> planner.plan(null, schema));
  }

  @Test
  void shouldRejectNullSchema() {
    assertThrows(NullPointerException.class, () -> planner.plan("SELECT id FROM users", null));
  }

  @Test
  void shouldGenerateLogicalProjectForSelectStatement() {
    SqlPlan plan = planner.plan("SELECT id, name FROM users", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof Project);
  }

  @Test
  void shouldGenerateLogicalFilterForWhereClause() {
    SqlPlan plan = planner.plan("SELECT id FROM users WHERE id > 10", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof Project);
    assertTrue(logicalPlan.getInput(0) instanceof Filter);
  }

  @Test
  void shouldGenerateProjectOverFilterForFilteredProjection() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof Project);
    assertTrue(logicalPlan.getInput(0) instanceof Filter);
  }

  @Test
  void shouldNotGenerateFilterWithoutWhereClause() {
    SqlPlan plan = planner.plan("SELECT id FROM users", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof Project);
    assertTrue(logicalPlan.getInputs().stream().noneMatch(Filter.class::isInstance));
  }

  @Test
  void shouldPrintLogicalPlanStructureForPhysicalTranslation() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    System.out.println("\n===== ASTRA DB LOGICAL PLAN =====");
    System.out.println(new SqlPlanExplainer().explain(plan));

    System.out.println("\n===== TABLE SCAN DETAILS =====");

    RelNode scanNode = logicalPlan;

    while (!(scanNode instanceof TableScan)) {
      if (scanNode.getInputs().isEmpty()) {
        throw new IllegalStateException("No TableScan found in logical plan");
      }

      scanNode = scanNode.getInput(0);
    }

    TableScan tableScan = (TableScan) scanNode;

    System.out.println("Scan class: " + tableScan.getClass().getName());
    System.out.println("Qualified name: " + tableScan.getTable().getQualifiedName());
    System.out.println("Scan row type: " + tableScan.getRowType());
    System.out.println("Table object: " + tableScan.getTable().getClass().getName());

    System.out.println("\n===== RELATIONAL NODE DETAILS =====");

    RelNode current = logicalPlan;

    while (true) {
      System.out.println("\nNode: " + current.getClass().getName());
      System.out.println("Inputs: " + current.getInputs().size());
      System.out.println("Row type: " + current.getRowType());

      if (current instanceof TableScan tableScanNode) {
        System.out.println("Qualified table name: " + tableScanNode.getTable().getQualifiedName());
      }

      if (current.getInputs().isEmpty()) {
        break;
      }

      current = current.getInput(0);
    }
  }

  @Test
  void shouldPrintSortPlanStructureForPhysicalTranslation() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10 ORDER BY id ASC", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    System.out.println("\n===== ORDER BY LOGICAL PLAN =====");
    System.out.println(new SqlPlanExplainer().explain(plan));

    System.out.println("\n===== SORT NODE DETAILS =====");

    RelNode current = logicalPlan;

    while (true) {
      System.out.println("\nNode: " + current.getClass().getName());
      System.out.println("Row type: " + current.getRowType());

      if (current instanceof org.apache.calcite.rel.core.Sort sort) {
        System.out.println("Collation: " + sort.getCollation());
        System.out.println("Field collations: " + sort.getCollation().getFieldCollations());
        System.out.println("Offset: " + sort.offset);
        System.out.println("Fetch: " + sort.fetch);
      }

      if (current.getInputs().isEmpty()) {
        break;
      }

      current = current.getInput(0);
    }
  }

  @Test
  void shouldInspectSortOutputAndValidatedRowType() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10 ORDER BY id ASC", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    System.out.println("\n===== SORT OUTPUT METADATA =====");
    System.out.println("Root row type: " + logicalPlan.getRowType());
    System.out.println("Root field count: " + logicalPlan.getRowType().getFieldCount());

    System.out.println("\n===== VALIDATED SQL METADATA =====");
    System.out.println("Validated node: " + plan.validatedStatement().getClass().getName());
    System.out.println("Validated SQL: " + plan.validatedStatement());
  }
}
