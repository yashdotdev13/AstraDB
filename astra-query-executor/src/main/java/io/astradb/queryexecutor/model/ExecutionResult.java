package io.astradb.queryexecutor.model;

import io.astradb.storage.mvcc.TransactionId;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the outcome of an executed operation.
 *
 * @param <V> result value type
 */
public final class ExecutionResult<V> {

  private final ExecutionOperation operation;
  private final TransactionId transactionId;
  private final Optional<V> value;

  private ExecutionResult(
      ExecutionOperation operation, TransactionId transactionId, Optional<V> value) {
    this.operation = Objects.requireNonNull(operation, "Operation must not be null");
    this.transactionId = Objects.requireNonNull(transactionId, "Transaction ID must not be null");
    this.value = Objects.requireNonNull(value, "Result value must not be null");
  }

  /**
   * Creates a result for an operation completed without a return value.
   *
   * @param operation completed operation
   * @param transactionId associated transaction ID
   * @param <V> result value type
   * @return completed execution result
   */
  public static <V> ExecutionResult<V> completed(
      ExecutionOperation operation, TransactionId transactionId) {
    return new ExecutionResult<>(operation, transactionId, Optional.empty());
  }

  public static <V> ExecutionResult<V> withValue(
      ExecutionOperation operation, TransactionId transactionId, V value) {
    return new ExecutionResult<>(
        operation,
        transactionId,
        Optional.of(Objects.requireNonNull(value, "Result value must not be null")));
  }

  public ExecutionOperation getOperation() {
    return operation;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public Optional<V> getValue() {
    return value;
  }
}
