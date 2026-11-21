package io.astradb.sql.model;

import java.util.Objects;
import org.apache.calcite.rel.RelNode;

/**
 * Represents the result of optimizing a SQL logical plan.
 *
 * @param originalPlan original logical relational plan
 * @param optimizedPlan optimized logical relational plan
 */
public record SqlOptimizationResult(RelNode originalPlan, RelNode optimizedPlan) {

  public SqlOptimizationResult {
    Objects.requireNonNull(originalPlan, "Original plan must not be null");
    Objects.requireNonNull(optimizedPlan, "Optimized plan must not be null");
  }
}
