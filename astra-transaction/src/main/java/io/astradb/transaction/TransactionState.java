package io.astradb.transaction;

/** Represents the lifecycle state of a transaction. */
public enum TransactionState {
  ACTIVE,
  COMMITTING,
  COMMITTED,
  ABORTED,
  IN_DOUBT
}
