package io.astradb.queryexecutor.execution;

import io.astradb.queryexecutor.dml.DmlExecutionResult;
import io.astradb.queryexecutor.dml.DmlExecutor;
import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.model.SqlExecutionResult;
import io.astradb.queryexecutor.plan.PhysicalPlanExecutor;
import io.astradb.queryexecutor.plan.PhysicalPlanTranslator;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlStatementType;
import io.astradb.sql.planning.SqlPlanner;
import io.astradb.transaction.Transaction;
import java.util.Objects;

/** Coordinates SQL planning and execution. */
public final class SqlExecutionCoordinator {

  private final SqlPlanner sqlPlanner;
  private final TransactionBackedRelationalTableAccess tableAccess;
  private final PhysicalPlanExecutor physicalPlanExecutor;
  private final DmlExecutor dmlExecutor;

  public SqlExecutionCoordinator(
      SqlPlanner sqlPlanner, TransactionBackedRelationalTableAccess tableAccess) {

    this.sqlPlanner = Objects.requireNonNull(sqlPlanner, "SQL planner must not be null");
    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");

    this.physicalPlanExecutor = new PhysicalPlanExecutor();
    this.dmlExecutor = new DmlExecutor(tableAccess);
  }

  /**
   * Plans and executes a SQL statement within an existing transaction.
   *
   * <p>Transaction lifecycle remains the responsibility of the caller.
   *
   * @param sql SQL statement
   * @param schema database schema
   * @param transaction active transaction
   * @return SQL execution result
   */
  public SqlExecutionResult execute(
      String sql, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    Objects.requireNonNull(sql, "SQL statement must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");
    Objects.requireNonNull(transaction, "Transaction must not be null");

    SqlPlan plan = sqlPlanner.plan(sql, schema);

    return switch (plan.statementType()) {
      case SELECT -> executeSelect(plan, schema, transaction);
      case INSERT -> executeInsert(plan, schema, transaction);
      case UPDATE -> executeUpdate(plan, schema, transaction);
      default ->
          throw new QueryExecutionException(
              "Unsupported SQL statement type: " + plan.statementType());
    };
  }

  private SqlExecutionResult executeSelect(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    PhysicalPlanTranslator translator =
        new PhysicalPlanTranslator(tableAccess, transaction, schema);

    return SqlExecutionResult.query(physicalPlanExecutor.execute(translator.translate(plan)));
  }

  private SqlExecutionResult executeInsert(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, transaction);

    return SqlExecutionResult.modification(SqlStatementType.INSERT, result.affectedRows());
  }

  private SqlExecutionResult executeUpdate(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    DmlExecutionResult result = dmlExecutor.execute(plan, schema, transaction);

    return SqlExecutionResult.modification(SqlStatementType.UPDATE, result.affectedRows());
  }
}
