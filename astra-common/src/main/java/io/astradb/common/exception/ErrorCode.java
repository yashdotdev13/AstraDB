package io.astradb.common.exception;

/**
 * Defines stable error code used throughout AstraDB
 *
 * <p>Error code are grouped by subsystem to make errors identifiable across module boundaries and
 * suitable for client facing response
 */
public enum ErrorCode {

  // General errors: 1xxx
  INTERNAL_ERROR(1000),
  INVALID_ARGUMENT(1001),
  INVALID_STATE(1002),
  UNSUPPORTED_OPERATION(1003),

  // Storage errors: 2xxx
  STORAGE_ERROR(2000),
  STORAGE_NOT_FOUND(2001),
  STORAGE_CORRUPTION(2002),

  // SQL errors: 3xxx
  SQL_ERROR(3000),
  SQL_PARSE_ERROR(3001),
  SQL_VALIDATION_ERROR(3002),
  SQL_EXECUTION_ERROR(3003),

  // Transaction errors: 4xxx
  TRANSACTION_ERROR(4000),
  TRANSACTION_ABORTED(4001),
  TRANSACTION_CONFLICT(4002),
  TRANSACTION_TIMEOUT(4003),

  // Distributed system errors: 5xxx
  DISTRIBUTED_ERROR(5000),
  NODE_UNAVAILABLE(5001),
  NOT_LEADER(5002),
  QUORUM_UNAVAILABLE(5003),
  REQUEST_TIMEOUT(5004);

  private final int code;

  ErrorCode(int code) {
    this.code = code;
  }

  /**
   * Returns the numeric identifier for this error.
   *
   * @return numeric error code
   */
  public int getCode() {
    return code;
  }
}
