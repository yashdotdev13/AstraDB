package io.astradb.transaction;

/** Indicates an error during transaction processing. */
public class TransactionException extends RuntimeException {

  public TransactionException(String message) {
    super(message);
  }

  public TransactionException(String message, Throwable cause) {
    super(message, cause);
  }
}
