package io.astradb.sql.planning;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqlPlanExplainerTest {

  private SqlPlanner planner;
  private SqlPlanExplainer explainer;
  private SqlSchema schema;

  @BeforeEach
  void setUp() {
    planner = new SqlPlanner();
    explainer = new SqlPlanExplainer();

    schema =
        new SqlSchema(
            List.of(
                new SqlTable(
                    "users",
                    List.of(
                        new SqlColumn("id", "INTEGER", false),
                        new SqlColumn("name", "VARCHAR", true))),
                new SqlTable(
                    "orders",
                    List.of(
                        new SqlColumn("id", "INTEGER", false),
                        new SqlColumn("user_id", "INTEGER", false),
                        new SqlColumn("amount", "DECIMAL", false)))));
  }

  @Test
  void shouldExplainLogicalPlanForSelectStatement() {
    SqlPlan plan = planner.plan("SELECT name FROM users WHERE id > 10", schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalProject"));
    assertTrue(explanation.contains("LogicalFilter"));
    assertTrue(explanation.contains("LogicalTableScan"));
  }

  @Test
  void shouldExplainSimpleSelectStatement() {
    SqlPlan plan = planner.plan("SELECT id FROM users", schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalProject"));
    assertTrue(explanation.contains("LogicalTableScan"));
  }

  @Test
  void shouldRejectPlanWithoutLogicalRelationalPlan() {
    SqlPlan plan = planner.plan("INSERT INTO users (id, name) VALUES (1, 'Yash')", schema);

    assertThrows(IllegalArgumentException.class, () -> explainer.explain(plan));
  }

  @Test
  void shouldRejectNullSqlPlan() {
    assertThrows(NullPointerException.class, () -> explainer.explain(null));
  }

  @Test
  void shouldProduceExplanationWithoutThrowing() {
    SqlPlan plan = planner.plan("SELECT id FROM users", schema);

    assertDoesNotThrow(() -> explainer.explain(plan));
  }

  @Test
  void shouldExplainLogicalPlanForJoinStatement() {
    SqlPlan plan =
        planner.plan(
            "SELECT users.name, orders.amount "
                + "FROM users JOIN orders ON users.id = orders.user_id",
            schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalProject"));
    assertTrue(explanation.contains("LogicalJoin"));
    assertTrue(explanation.contains("LogicalTableScan"));
  }

  @Test
  void shouldExplainLogicalPlanForAggregation() {
    SqlPlan plan = planner.plan("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id", schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalAggregate"));
    assertTrue(explanation.contains("LogicalTableScan"));
  }

  @Test
  void shouldExplainLogicalPlanForOrderBy() {
    SqlPlan plan = planner.plan("SELECT id, name FROM users ORDER BY name", schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalSort"));
    assertTrue(explanation.contains("LogicalProject"));
  }

  @Test
  void shouldExplainLogicalPlanForTableAlias() {
    SqlPlan plan = planner.plan("SELECT u.name FROM users u WHERE u.id > 10", schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalProject"));
    assertTrue(explanation.contains("LogicalFilter"));
    assertTrue(explanation.contains("LogicalTableScan"));
  }

  @Test
  void shouldExplainLogicalPlanForColumnAlias() {
    SqlPlan plan = planner.plan("SELECT name AS user_name FROM users", schema);

    String explanation = explainer.explain(plan);

    assertTrue(explanation.contains("LogicalProject"));
    assertTrue(explanation.contains("USER_NAME"));
  }
}
