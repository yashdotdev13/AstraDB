package io.astradb.common.exception;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link AstraDBException}. */
class AstraDBExceptionTest {

  @Test
  void shouldPreserveErrorCodeAndMessage() {
    AstraDBException exception =
        new AstraDBException(ErrorCode.INVALID_ARGUMENT, "Invalid request");

    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
    assertEquals(1001, exception.getCode());
    assertEquals("Invalid request", exception.getMessage());
  }

  @Test
  void shouldPreserveRootCause() {
    RuntimeException cause = new RuntimeException("Underlying failure");

    AstraDBException exception =
        new AstraDBException(ErrorCode.INTERNAL_ERROR, "Operation failed", cause);

    assertEquals(ErrorCode.INTERNAL_ERROR, exception.getErrorCode());
    assertEquals(1000, exception.getCode());
    assertSame(cause, exception.getCause());
  }

  @Test
  void errorCodesShouldHaveStableNumericIdentifiers() {
    assertEquals(1000, ErrorCode.INTERNAL_ERROR.getCode());
    assertEquals(2000, ErrorCode.STORAGE_ERROR.getCode());
    assertEquals(3000, ErrorCode.SQL_ERROR.getCode());
    assertEquals(4000, ErrorCode.TRANSACTION_ERROR.getCode());
    assertEquals(5000, ErrorCode.DISTRIBUTED_ERROR.getCode());
  }

  @Test
  void exceptionShouldBeUnchecked() {
    AstraDBException exception = new AstraDBException(ErrorCode.STORAGE_ERROR, "Storage failure");

    assertNotNull(exception);
    assertEquals(RuntimeException.class, exception.getClass().getSuperclass());
  }

  @Test
  void shouldRejectNullErrorCode() {
    assertThrows(NullPointerException.class, () -> new AstraDBException(null, "Invalid error"));
  }
}
