package io.astradb.sql.planning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlOptimizationResult;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqlOptimizerTest {

  private SqlPlanner planner;
  private SqlOptimizer optimizer;
  private SqlSchema schema;

  @BeforeEach
  void setUp() {
    planner = new SqlPlanner();
    optimizer = new SqlOptimizer();

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
  void shouldOptimizeSelectLogicalPlan() {
    SqlPlan plan = planner.plan("SELECT id, name FROM users WHERE id > 10", schema);

    SqlOptimizationResult result = optimizer.optimize(plan);

    assertNotNull(result);
    assertNotNull(result.originalPlan());
    assertNotNull(result.optimizedPlan());
  }

  @Test
  void shouldPreserveOriginalLogicalPlan() {
    SqlPlan plan = planner.plan("SELECT id FROM users WHERE id > 10", schema);

    SqlOptimizationResult result = optimizer.optimize(plan);

    assertEquals(
        RelOptUtil.toString(plan.logicalPlan().orElseThrow()),
        RelOptUtil.toString(result.originalPlan()));
  }

  @Test
  void shouldProduceRelationalPlanAfterOptimization() {
    SqlPlan plan = planner.plan("SELECT id FROM users", schema);

    SqlOptimizationResult result = optimizer.optimize(plan);

    assertNotNull(result.optimizedPlan().getRowType());
    assertTrue(result.optimizedPlan().getInputs().size() >= 0);
  }

  @Test
  void shouldRejectSqlPlanWithoutLogicalPlan() {
    SqlPlan plan = planner.plan("INSERT INTO users (id, name) VALUES (1, 'Yash')", schema);

    assertThrows(IllegalArgumentException.class, () -> optimizer.optimize(plan));
  }

  @Test
  void shouldRejectNullSqlPlan() {
    assertThrows(NullPointerException.class, () -> optimizer.optimize(null));
  }

  @Test
  void shouldAcceptCustomOptimizationConfiguration() {
    SqlOptimizationConfig config = new SqlOptimizationConfig(false, false, false, false);
    SqlOptimizer configuredOptimizer = new SqlOptimizer(config);

    SqlPlan plan = planner.plan("SELECT id FROM users", schema);

    SqlOptimizationResult result = configuredOptimizer.optimize(plan);

    assertNotNull(result.optimizedPlan());
  }

  @Test
  void shouldReduceConstantExpressionsInFilter() {
    SqlPlan plan = planner.plan("SELECT id FROM users WHERE id > 10 AND 1 = 1", schema);

    SqlOptimizationResult result = optimizer.optimize(plan);

    String original = RelOptUtil.toString(result.originalPlan());
    String optimized = RelOptUtil.toString(result.optimizedPlan());

    assertTrue(original.contains("=(1, 1)"));
    assertTrue(original.contains("AND"));

    assertTrue(optimized.contains("LogicalFilter"));
    assertTrue(optimized.contains(">($0, 10)"));
    assertFalse(optimized.contains("=(1, 1)"));
    assertFalse(optimized.contains("AND"));
  }

  @Test
  void shouldOptimizeFilterOverProjection() {
    SqlPlan plan = planner.plan("SELECT id FROM users WHERE id > 10", schema);

    SqlOptimizationResult result = optimizer.optimize(plan);

    String optimized = RelOptUtil.toString(result.optimizedPlan());

    assertTrue(optimized.contains("LogicalProject"));
    assertTrue(optimized.contains("LogicalFilter"));
    assertTrue(optimized.contains("LogicalTableScan"));
  }

  @Test
  void shouldMergeConsecutiveProjectOperators() {
    SqlPlan basePlan = planner.plan("SELECT id, name FROM users", schema);

    RelNode originalProject = basePlan.logicalPlan().orElseThrow();
    RelNode tableScan = originalProject.getInput(0);

    LogicalProject firstProject =
        LogicalProject.create(
            tableScan,
            List.of(),
            List.of(
                RexInputRef.of(0, tableScan.getRowType()),
                RexInputRef.of(1, tableScan.getRowType())),
            List.of("ID", "NAME"));

    LogicalProject secondProject =
        LogicalProject.create(
            firstProject,
            List.of(),
            List.of(RexInputRef.of(1, firstProject.getRowType())),
            List.of("NAME"));

    SqlPlan nestedPlan =
        new SqlPlan(
            basePlan.sql(),
            basePlan.statementType(),
            basePlan.validatedStatement(),
            Optional.of(secondProject));

    SqlOptimizationResult result = optimizer.optimize(nestedPlan);

    String original = RelOptUtil.toString(result.originalPlan());
    String optimized = RelOptUtil.toString(result.optimizedPlan());

    long originalProjectCount = original.split("LogicalProject", -1).length - 1;
    long optimizedProjectCount = optimized.split("LogicalProject", -1).length - 1;

    assertEquals(2, originalProjectCount, "Original plan must contain two projections");
    assertEquals(1, optimizedProjectCount, "Optimized plan must contain one projection");
  }

  @Test
  void shouldMergeConsecutiveFilterOperators() {
    SqlPlan basePlan = planner.plan("SELECT id FROM users", schema);

    RelNode originalPlan = basePlan.logicalPlan().orElseThrow();
    RelNode tableScan = originalPlan.getInput(0);

    RexBuilder rexBuilder = tableScan.getCluster().getRexBuilder();

    RexNode firstCondition =
        rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN,
            RexInputRef.of(0, tableScan.getRowType()),
            rexBuilder.makeExactLiteral(java.math.BigDecimal.TEN));

    RexNode secondCondition =
        rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN,
            RexInputRef.of(0, tableScan.getRowType()),
            rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(100)));

    RelNode firstFilter =
        org.apache.calcite.rel.logical.LogicalFilter.create(tableScan, firstCondition);

    RelNode secondFilter =
        org.apache.calcite.rel.logical.LogicalFilter.create(firstFilter, secondCondition);

    SqlPlan nestedPlan =
        new SqlPlan(
            basePlan.sql(),
            basePlan.statementType(),
            basePlan.validatedStatement(),
            Optional.of(secondFilter));

    SqlOptimizationResult result = optimizer.optimize(nestedPlan);

    String original = RelOptUtil.toString(result.originalPlan());
    String optimized = RelOptUtil.toString(result.optimizedPlan());

    assertEquals(2, original.split("LogicalFilter", -1).length - 1);
    assertEquals(1, optimized.split("LogicalFilter", -1).length - 1);
  }

  @Test
  void shouldTransposeFilterBelowProject() {
    SqlPlan basePlan = planner.plan("SELECT id, name FROM users", schema);

    RelNode tableScan = basePlan.logicalPlan().orElseThrow().getInput(0);

    LogicalProject project =
        LogicalProject.create(
            tableScan,
            List.of(),
            List.of(
                RexInputRef.of(0, tableScan.getRowType()),
                RexInputRef.of(1, tableScan.getRowType())),
            List.of("ID", "NAME"));

    RexBuilder rexBuilder = tableScan.getCluster().getRexBuilder();

    RexNode condition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            RexInputRef.of(0, project.getRowType()),
            rexBuilder.makeExactLiteral(BigDecimal.TEN));

    RelNode filter = LogicalFilter.create(project, condition);

    SqlPlan nestedPlan =
        new SqlPlan(
            basePlan.sql(),
            basePlan.statementType(),
            basePlan.validatedStatement(),
            Optional.of(filter));

    SqlOptimizationResult result = optimizer.optimize(nestedPlan);

    RelNode optimizedPlan = result.optimizedPlan();

    assertTrue(optimizedPlan instanceof LogicalProject);
    assertTrue(optimizedPlan.getInput(0) instanceof LogicalFilter);
  }

  @Test
  void shouldRespectDisabledProjectMergeRule() {
    SqlPlan basePlan = planner.plan("SELECT id, name FROM users", schema);

    RelNode tableScan = basePlan.logicalPlan().orElseThrow().getInput(0);

    LogicalProject firstProject =
        LogicalProject.create(
            tableScan,
            List.of(),
            List.of(
                RexInputRef.of(0, tableScan.getRowType()),
                RexInputRef.of(1, tableScan.getRowType())),
            List.of("ID", "NAME"));

    LogicalProject secondProject =
        LogicalProject.create(
            firstProject,
            List.of(),
            List.of(RexInputRef.of(1, firstProject.getRowType())),
            List.of("NAME"));

    SqlPlan nestedPlan =
        new SqlPlan(
            basePlan.sql(),
            basePlan.statementType(),
            basePlan.validatedStatement(),
            Optional.of(secondProject));

    SqlOptimizer optimizerWithoutProjectMerge =
        new SqlOptimizer(new SqlOptimizationConfig(true, false, true, true));

    SqlOptimizationResult result = optimizerWithoutProjectMerge.optimize(nestedPlan);

    String optimized = RelOptUtil.toString(result.optimizedPlan());

    long optimizedProjectCount = optimized.split("LogicalProject", -1).length - 1;

    assertEquals(
        2,
        optimizedProjectCount,
        "Consecutive projects should remain when project merge is disabled");
  }
}
