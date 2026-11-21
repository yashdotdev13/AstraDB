package io.astradb.querycoordinator.model;

import io.astradb.common.identity.RequestId;
import io.astradb.queryexecutor.model.ExecutionResult;
import java.util.Objects;

/**
 * Represents the response produced after coordinating query execution.
 *
 * @param <V> result value type
 */
public final class QueryExecutionResponse<V> {

  private final RequestId requestId;
  private final ExecutionResult<V> executionResult;

  /**
   * Creates a query execution response.
   *
   * @param requestId correlation ID of the query request
   * @param executionResult result returned by the query executor
   */
  public QueryExecutionResponse(RequestId requestId, ExecutionResult<V> executionResult) {
    this.requestId = Objects.requireNonNull(requestId, "Request ID must not be null");
    this.executionResult =
        Objects.requireNonNull(executionResult, "Execution result must not be null");
  }

  public RequestId getRequestId() {
    return requestId;
  }

  public ExecutionResult<V> getExecutionResult() {
    return executionResult;
  }
}
