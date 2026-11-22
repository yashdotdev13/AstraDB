package io.astradb.queryexecutor.plan;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Executes a composed physical relational operator tree.
 *
 * <p>The executor owns the lifecycle of the supplied root operator and materializes its output into
 * an immutable list.
 */
public final class PhysicalPlanExecutor {

  /**
   * Executes a physical operator tree and returns all produced rows.
   *
   * @param rootOperator root of the physical operator tree
   * @return immutable list of output rows
   */
  public List<RelationalRow> execute(PhysicalOperator rootOperator) {
    Objects.requireNonNull(rootOperator, "Root operator must not be null");

    try {
      return collectRows(rootOperator);
    } finally {
      rootOperator.close();
    }
  }

  private List<RelationalRow> collectRows(PhysicalOperator rootOperator) {
    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = rootOperator.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }

    return List.copyOf(rows);
  }
}
