package io.astradb.sql.exception;

import io.astradb.common.exception.AstraDBException;
import io.astradb.common.exception.ErrorCode;

/** Exception thrown when a SQL statement cannot be parsed. */
public class SqlParseException extends AstraDBException {

  public SqlParseException(String message, Throwable cause) {
    super(ErrorCode.SQL_PARSE_ERROR, message, cause);
  }
}
