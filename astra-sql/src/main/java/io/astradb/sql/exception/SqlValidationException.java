package io.astradb.sql.exception;

import io.astradb.common.exception.AstraDBException;
import io.astradb.common.exception.ErrorCode;

/** Exception thrown when a SQL statement fails semantic validation. */
public class SqlValidationException extends AstraDBException {

  public SqlValidationException(String message, Throwable cause) {
    super(ErrorCode.SQL_VALIDATION_ERROR, message, cause);
  }
}
