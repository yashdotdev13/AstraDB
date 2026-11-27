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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
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
                        new SqlColumn("name", "VARCHAR", true))),
                new SqlTable(
                    "incoming_users",
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
  void shouldCreateLogicalTableModifyPlanForUpdateStatement() {
    SqlPlan plan = planner.plan("UPDATE users SET name = 'Updated' WHERE id = 1", schema);

    assertEquals(SqlStatementType.UPDATE, plan.statementType());

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof TableModify);

    TableModify modify = (TableModify) logicalPlan;

    assertEquals(TableModify.Operation.UPDATE, modify.getOperation());
    assertEquals(List.of("NAME"), modify.getUpdateColumnList());
    assertNotNull(modify.getSourceExpressionList());
    assertTrue(modify.getInput() instanceof Project);
    assertTrue(modify.getInput().getInput(0) instanceof Filter);
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
  void shouldCreateLogicalMergePlanForMergeStatement() {
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

    SqlPlan plan = planner.plan(sql, schema);

    assertEquals(SqlStatementType.MERGE, plan.statementType());
    assertNotNull(plan.validatedStatement());

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof TableModify);

    TableModify modify = (TableModify) logicalPlan;

    assertEquals(TableModify.Operation.MERGE, modify.getOperation());
    assertEquals(List.of("NAME"), modify.getUpdateColumnList());

    assertTrue(modify.getInput() instanceof Project);

    Project project = (Project) modify.getInput();

    assertTrue(project.getInput() instanceof Join);

    Join join = (Join) project.getInput();

    assertEquals(org.apache.calcite.rel.core.JoinRelType.LEFT, join.getJoinType());
    assertTrue(join.getLeft() instanceof TableScan);
    assertTrue(join.getRight() instanceof TableScan);
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
  void shouldGenerateLogicalPlanWithTableScanForFilteredProjection() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof Project);

    RelNode current = logicalPlan;

    while (!(current instanceof TableScan)) {
      assertTrue(!current.getInputs().isEmpty());
      current = current.getInput(0);
    }

    TableScan tableScan = (TableScan) current;

    assertEquals(List.of("ASTRA", "USERS"), tableScan.getTable().getQualifiedName());
    assertEquals(2, tableScan.getRowType().getFieldCount());
  }

  @Test
  void shouldGenerateSortPlanForOrderByClause() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10 ORDER BY id ASC", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertTrue(logicalPlan instanceof Sort);

    Sort sort = (Sort) logicalPlan;

    assertNotNull(sort.getCollation());
    assertEquals(1, sort.getCollation().getFieldCollations().size());
    assertTrue(sort.getInput() instanceof Project);
  }

  @Test
  void shouldPreserveSortOutputMetadata() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10 ORDER BY id ASC", schema);

    RelNode logicalPlan = plan.logicalPlan().orElseThrow();

    assertEquals(2, logicalPlan.getRowType().getFieldCount());
    assertNotNull(plan.validatedStatement());
    assertEquals(SqlStatementType.SELECT, plan.statementType());
  }
}
