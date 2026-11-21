package io.astradb.querycoordinator.model;

import io.astradb.common.identity.RequestId;
import io.astradb.queryexecutor.model.ExecutionRequest;
import java.util.Objects;

/**
 * Represents an immutable query plan submitted for coordination.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class QueryPlan<K, V> {

  private final RequestId requestId;
  private final ExecutionRequest<K, V> executionRequest;

  private QueryPlan(RequestId requestId, ExecutionRequest<K, V> executionRequest) {
    this.requestId = Objects.requireNonNull(requestId, "Request ID must not be null");
    this.executionRequest =
        Objects.requireNonNull(executionRequest, "Execution request must not be null");
  }

  /**
   * Creates a query plan with a newly generated request ID.
   *
   * @param executionRequest execution request
   * @param <K> key type
   * @param <V> value type
   * @return newly created query plan
   */
  public static <K, V> QueryPlan<K, V> create(ExecutionRequest<K, V> executionRequest) {
    return new QueryPlan<>(RequestId.generate(), executionRequest);
  }

  public RequestId getRequestId() {
    return requestId;
  }

  public ExecutionRequest<K, V> getExecutionRequest() {
    return executionRequest;
  }
}
