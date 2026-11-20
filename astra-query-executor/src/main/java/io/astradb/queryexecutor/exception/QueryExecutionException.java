package io.astradb.queryexecutor.exception;

import io.astradb.common.exception.AstraDBException;
import io.astradb.common.exception.ErrorCode;

/** Represents a failure during query execution. */
public final class QueryExecutionException extends AstraDBException {

  public QueryExecutionException(String message) {
    super(ErrorCode.SQL_EXECUTION_ERROR, message);
  }

  public QueryExecutionException(String message, Throwable cause) {
    super(ErrorCode.SQL_EXECUTION_ERROR, message, cause);
  }
}
