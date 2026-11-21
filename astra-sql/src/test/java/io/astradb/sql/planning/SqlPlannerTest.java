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
  void shouldCreatePlanForUpdateStatement() {
    String sql = "UPDATE users SET name = 'Updated' WHERE id = 1";

    SqlPlan plan = planner.plan(sql, schema);

    assertEquals(SqlStatementType.UPDATE, plan.statementType());
    assertTrue(plan.logicalPlan().isEmpty());
  }

  @Test
  void shouldCreatePlanForDeleteStatement() {
    String sql = "DELETE FROM users WHERE id = 1";

    SqlPlan plan = planner.plan(sql, schema);

    assertEquals(SqlStatementType.DELETE, plan.statementType());
    assertTrue(plan.logicalPlan().isEmpty());
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
}
