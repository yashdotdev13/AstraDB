package io.astradb.queryexecutor.dml;

import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlStatementType;
import io.astradb.transaction.Transaction;
import java.util.Objects;

/** Executes SQL data manipulation statements. */
public final class DmlExecutor {

  private final InsertExecutor insertExecutor;

  public DmlExecutor(TransactionBackedRelationalTableAccess tableAccess) {
    Objects.requireNonNull(tableAccess, "Table access must not be null");
    this.insertExecutor = new InsertExecutor(tableAccess);
  }

  public DmlExecutionResult execute(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    Objects.requireNonNull(plan, "SQL plan must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");
    Objects.requireNonNull(transaction, "Transaction must not be null");

    if (plan.statementType() == SqlStatementType.INSERT) {
      return insertExecutor.execute(plan, schema, transaction);
    }

    throw new QueryExecutionException("Unsupported DML statement type: " + plan.statementType());
  }
}
