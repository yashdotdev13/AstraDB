package io.astradb.queryexecutor.model;

import io.astradb.storage.mvcc.TransactionId;
import java.util.Objects;

/**
 * Represents an operation submitted to the query execution layer.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ExecutionRequest<K, V> {

  private final ExecutionOperation operation;
  private final TransactionId transactionId;
  private final K key;
  private final V value;

  private ExecutionRequest(
      ExecutionOperation operation, TransactionId transactionId, K key, V value) {
    this.operation = Objects.requireNonNull(operation, "Operation must not be null");
    this.transactionId = transactionId;
    this.key = key;
    this.value = value;
  }

  public static <K, V> ExecutionRequest<K, V> begin() {
    return new ExecutionRequest<>(ExecutionOperation.BEGIN, null, null, null);
  }

  public static <K, V> ExecutionRequest<K, V> get(TransactionId transactionId, K key) {
    return new ExecutionRequest<>(
        ExecutionOperation.GET,
        Objects.requireNonNull(transactionId, "Transaction ID must not be null"),
        Objects.requireNonNull(key, "Key must not be null"),
        null);
  }

  public static <K, V> ExecutionRequest<K, V> put(TransactionId transactionId, K key, V value) {
    return new ExecutionRequest<>(
        ExecutionOperation.PUT,
        Objects.requireNonNull(transactionId, "Transaction ID must not be null"),
        Objects.requireNonNull(key, "Key must not be null"),
        Objects.requireNonNull(value, "Value must not be null"));
  }

  public static <K, V> ExecutionRequest<K, V> delete(TransactionId transactionId, K key) {
    return new ExecutionRequest<>(
        ExecutionOperation.DELETE,
        Objects.requireNonNull(transactionId, "Transaction ID must not be null"),
        Objects.requireNonNull(key, "Key must not be null"),
        null);
  }

  public static <K, V> ExecutionRequest<K, V> commit(TransactionId transactionId) {
    return new ExecutionRequest<>(
        ExecutionOperation.COMMIT,
        Objects.requireNonNull(transactionId, "Transaction ID must not be null"),
        null,
        null);
  }

  public static <K, V> ExecutionRequest<K, V> rollback(TransactionId transactionId) {
    return new ExecutionRequest<>(
        ExecutionOperation.ROLLBACK,
        Objects.requireNonNull(transactionId, "Transaction ID must not be null"),
        null,
        null);
  }

  public ExecutionOperation getOperation() {
    return operation;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }
}
