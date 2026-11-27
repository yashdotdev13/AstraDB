package io.astradb.queryexecutor.dml;

import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.plan.RexExpressionEvaluator;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;

/** Executes DELETE statements using transaction-backed relational storage. */
public final class DeleteExecutor {

  private final TransactionBackedRelationalTableAccess tableAccess;
  private final RexExpressionEvaluator evaluator;

  public DeleteExecutor(TransactionBackedRelationalTableAccess tableAccess) {
    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");
    this.evaluator = new RexExpressionEvaluator();
  }

  public DmlExecutionResult execute(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    Objects.requireNonNull(plan, "SQL plan must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");
    Objects.requireNonNull(transaction, "Transaction must not be null");

    if (!(plan.logicalPlan().orElse(null) instanceof TableModify modify)
        || modify.getOperation() != TableModify.Operation.DELETE) {
      throw new QueryExecutionException("DELETE logical plan is missing or invalid");
    }

    SqlTable table = resolveTable(modify, schema);
    Filter filter = resolveFilter(modify);

    // Identify every matching key before staging any deletions.
    List<String> keysToDelete = new ArrayList<>();

    for (Map.Entry<String, RelationalRow> entry :
        tableAccess.scanEntries(transaction, table).entrySet()) {

      if (filter != null) {
        Object matches = evaluator.evaluate(filter.getCondition(), entry.getValue());

        // SQL WHERE selects only rows for which the condition is TRUE.
        if (!Boolean.TRUE.equals(matches)) {
          continue;
        }
      }

      keysToDelete.add(entry.getKey());
    }

    // The caller controls commit and rollback.
    for (String key : keysToDelete) {
      tableAccess.delete(transaction, table, key);
    }

    return new DmlExecutionResult(keysToDelete.size());
  }

  private SqlTable resolveTable(TableModify modify, SqlSchema schema) {
    List<String> qualifiedName = modify.getTable().getQualifiedName();

    if (qualifiedName.isEmpty()) {
      throw new QueryExecutionException("Invalid DELETE target table");
    }

    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    return schema
        .findTable(tableName)
        .orElseThrow(() -> new QueryExecutionException("Unknown table: " + tableName));
  }

  private Filter resolveFilter(TableModify modify) {
    RelNode input = modify.getInput();

    if (!(input instanceof Project project)) {
      throw new QueryExecutionException("Unsupported DELETE relational plan: expected Project");
    }

    RelNode projectInput = project.getInput();

    if (projectInput instanceof Filter filter) {
      if (!(filter.getInput() instanceof TableScan)) {
        throw new QueryExecutionException(
            "Unsupported DELETE relational plan: expected TableScan below Filter");
      }
      return filter;
    }

    if (projectInput instanceof TableScan) {
      return null;
    }

    throw new QueryExecutionException(
        "Unsupported DELETE relational plan: expected Filter or TableScan below Project");
  }
}
