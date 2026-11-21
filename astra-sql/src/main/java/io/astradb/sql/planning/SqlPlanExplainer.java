package io.astradb.sql.planning;

import io.astradb.sql.model.SqlPlan;
import java.util.Objects;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

/** Provides human-readable explanations of SQL logical plans. */
public class SqlPlanExplainer {

  /**
   * Converts a SQL plan into a readable relational plan representation.
   *
   * @param sqlPlan SQL plan to explain
   * @return textual representation of the logical plan
   * @throws IllegalArgumentException if the SQL plan has no logical plan
   */
  public String explain(SqlPlan sqlPlan) {
    Objects.requireNonNull(sqlPlan, "SQL plan must not be null");

    RelNode logicalPlan =
        sqlPlan
            .logicalPlan()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "SQL plan does not contain a logical relational plan"));

    return RelOptUtil.toString(logicalPlan);
  }
}
