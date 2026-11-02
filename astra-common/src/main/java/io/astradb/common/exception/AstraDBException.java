package io.astradb.common.exception;

import java.util.Objects;

/**
 * Base unchecked exception for AstraDB-specific failures.
 *
 * <p>Every exception carries a stable {@link ErrorCode} so that callers can handle failures
 * programmatically without relying on message text.
 */
public class AstraDBException extends RuntimeException {

  private final ErrorCode errorCode;

  /**
   * Creates an exception with an error code and descriptive message.
   *
   * @param errorCode stable AstraDB error code
   * @param message description of the failure
   */
  public AstraDBException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = Objects.requireNonNull(errorCode, "ErrorCode must not be null");
  }

  /**
   * Creates an exception with an error code, message, and root cause.
   *
   * @param errorCode stable AstraDB error code
   * @param message description of the failure
   * @param cause underlying cause
   */
  public AstraDBException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = Objects.requireNonNull(errorCode, "ErrorCode must not be null");
  }

  /**
   * Returns the stable error code associated with this exception.
   *
   * @return AstraDB error code
   */
  public ErrorCode getErrorCode() {
    return errorCode;
  }

  /**
   * Returns the numeric identifier associated with this exception.
   *
   * @return numeric error code
   */
  public int getCode() {
    return errorCode.getCode();
  }
}
