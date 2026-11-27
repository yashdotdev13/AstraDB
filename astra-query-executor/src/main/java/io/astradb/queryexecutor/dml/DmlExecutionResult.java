package io.astradb.queryexecutor.dml;

/**
 * Represents the result of executing a data manipulation statement.
 *
 * @param affectedRows number of rows affected by the statement
 */
public record DmlExecutionResult(int affectedRows) {

  public DmlExecutionResult {
    if (affectedRows < 0) {
      throw new IllegalArgumentException("Affected row count must not be negative");
    }
  }
}
