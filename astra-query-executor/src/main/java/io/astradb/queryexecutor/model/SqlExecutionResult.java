package io.astradb.queryexecutor.model;

import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlStatementType;
import java.util.List;
import java.util.Objects;

/** Represents the result of executing a SQL statement. */
public record SqlExecutionResult(
    SqlStatementType statementType, List<RelationalRow> rows, int affectedRows) {

  public SqlExecutionResult {
    Objects.requireNonNull(statementType, "Statement type must not be null");
    rows = List.copyOf(Objects.requireNonNull(rows, "Rows must not be null"));

    if (affectedRows < 0) {
      throw new IllegalArgumentException("Affected rows must not be negative");
    }
  }

  public static SqlExecutionResult query(List<RelationalRow> rows) {
    return new SqlExecutionResult(SqlStatementType.SELECT, rows, 0);
  }

  public static SqlExecutionResult modification(SqlStatementType statementType, int affectedRows) {
    return new SqlExecutionResult(statementType, List.of(), affectedRows);
  }
}
