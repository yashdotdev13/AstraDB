package io.astradb.sql.planning;

import io.astradb.sql.exception.SqlPlanningException;
import io.astradb.sql.model.SqlOptimizationResult;
import io.astradb.sql.model.SqlPlan;
import java.util.Objects;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;

/** Optimizes SQL logical relational plans using Apache Calcite. */
public class SqlOptimizer {

  private final SqlOptimizationConfig config;

  /** Creates an optimizer using the default optimization configuration. */
  public SqlOptimizer() {
    this(SqlOptimizationConfig.defaults());
  }

  /**
   * Creates an optimizer with the supplied configuration.
   *
   * @param config optimization configuration
   */
  public SqlOptimizer(SqlOptimizationConfig config) {
    this.config = Objects.requireNonNull(config, "Optimization config must not be null");
  }

  /**
   * Optimizes the logical relational plan contained in a SQL plan.
   *
   * @param sqlPlan SQL plan to optimize
   * @return original and optimized relational plans
   * @throws IllegalArgumentException if the SQL plan has no logical relational plan
   * @throws SqlPlanningException if optimization fails
   */
  public SqlOptimizationResult optimize(SqlPlan sqlPlan) {
    Objects.requireNonNull(sqlPlan, "SQL plan must not be null");

    RelNode originalPlan =
        sqlPlan
            .logicalPlan()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "SQL plan does not contain a logical relational plan"));

    try {
      HepProgramBuilder programBuilder = new HepProgramBuilder();

      if (config.enableFilterProjectTranspose()) {
        programBuilder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
      }

      if (config.enableProjectMerge()) {
        programBuilder.addRuleInstance(CoreRules.PROJECT_MERGE);
      }

      if (config.enableFilterMerge()) {
        programBuilder.addRuleInstance(CoreRules.FILTER_MERGE);
      }

      if (config.enableConstantReduction()) {
        programBuilder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        programBuilder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
      }

      HepProgram program = programBuilder.build();
      HepPlanner hepPlanner = new HepPlanner(program);

      hepPlanner.setRoot(originalPlan);
      RelNode optimizedPlan = hepPlanner.findBestExp();

      return new SqlOptimizationResult(originalPlan, optimizedPlan);
    } catch (RuntimeException exception) {
      throw new SqlPlanningException(
          "Failed to optimize SQL logical plan: " + exception.getMessage(), exception);
    }
  }
}
