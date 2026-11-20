package io.astradb.queryexecutor.execution;

import io.astradb.common.identity.RequestId;
import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.model.ExecutionOperation;
import io.astradb.queryexecutor.model.ExecutionRequest;
import io.astradb.queryexecutor.model.ExecutionResult;
import io.astradb.storage.mvcc.TransactionId;
import io.astradb.transaction.Transaction;
import io.astradb.transaction.TransactionManager;
import java.util.Objects;
import java.util.Optional;

/**
 * Executes transaction-aware key-value operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class QueryExecutor<K, V> {

  private final TransactionManager<K, V> transactionManager;

  public QueryExecutor(TransactionManager<K, V> transactionManager) {
    this.transactionManager =
        Objects.requireNonNull(transactionManager, "Transaction manager must not be null");
  }

  /**
   * Executes an operation and returns its result.
   *
   * @param request execution request
   * @return execution result
   */
  public ExecutionResult<V> execute(ExecutionRequest<K, V> request) {
    Objects.requireNonNull(request, "Execution request must not be null");

    ExecutionContext context =
        new ExecutionContext(RequestId.generate(), request.getTransactionId());

    try {
      return executeOperation(request, context);
    } catch (QueryExecutionException exception) {
      throw exception;
    } catch (RuntimeException exception) {
      throw new QueryExecutionException(
          "Failed to execute operation "
              + request.getOperation()
              + " for request "
              + context.getRequestId(),
          exception);
    }
  }

  private ExecutionResult<V> executeOperation(
      ExecutionRequest<K, V> request, ExecutionContext context) {

    ExecutionOperation operation = request.getOperation();

    switch (operation) {
      case BEGIN -> {
        Transaction<K, V> transaction = transactionManager.begin();
        return ExecutionResult.completed(operation, transaction.getId());
      }

      case GET -> {
        Transaction<K, V> transaction = getTransaction(context);
        Optional<V> value = transaction.get(request.getKey());

        if (value.isPresent()) {
          return ExecutionResult.withValue(operation, context.getTransactionId(), value.get());
        }

        return ExecutionResult.completed(operation, context.getTransactionId());
      }

      case PUT -> {
        Transaction<K, V> transaction = getTransaction(context);
        transaction.put(request.getKey(), request.getValue());
        return ExecutionResult.completed(operation, context.getTransactionId());
      }

      case DELETE -> {
        Transaction<K, V> transaction = getTransaction(context);
        transaction.delete(request.getKey());
        return ExecutionResult.completed(operation, context.getTransactionId());
      }

      case COMMIT -> {
        transactionManager.commit(context.getTransactionId());
        return ExecutionResult.completed(operation, context.getTransactionId());
      }

      case ROLLBACK -> {
        transactionManager.rollback(context.getTransactionId());
        return ExecutionResult.completed(operation, context.getTransactionId());
      }

      default -> throw new QueryExecutionException("Unsupported execution operation: " + operation);
    }
  }

  private Transaction<K, V> getTransaction(ExecutionContext context) {
    TransactionId transactionId = context.getTransactionId();

    if (transactionId == null) {
      throw new QueryExecutionException("Transaction ID is required for this operation");
    }

    return transactionManager.getTransaction(transactionId);
  }
}
