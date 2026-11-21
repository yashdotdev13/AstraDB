package io.astradb.sql.exception;

import io.astradb.common.exception.AstraDBException;
import io.astradb.common.exception.ErrorCode;

/** Exception thrown when a SQL statement cannot be converted into a query plan. */
public class SqlPlanningException extends AstraDBException {

  public SqlPlanningException(String message, Throwable cause) {
    super(ErrorCode.SQL_ERROR, message, cause);
  }
}
