package io.astradb.querycoordinator.exception;

import io.astradb.common.exception.AstraDBException;
import io.astradb.common.exception.ErrorCode;

/** Represents a failure during query coordination. */
public final class QueryCoordinatorException extends AstraDBException {

  public QueryCoordinatorException(String message) {
    super(ErrorCode.SQL_EXECUTION_ERROR, message);
  }

  public QueryCoordinatorException(String message, Throwable cause) {
    super(ErrorCode.SQL_EXECUTION_ERROR, message, cause);
  }
}
