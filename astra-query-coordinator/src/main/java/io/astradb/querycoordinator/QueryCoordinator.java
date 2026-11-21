package io.astradb.querycoordinator;

import io.astradb.querycoordinator.exception.QueryCoordinatorException;
import io.astradb.querycoordinator.model.QueryExecutionResponse;
import io.astradb.querycoordinator.model.QueryPlan;
import io.astradb.queryexecutor.execution.QueryExecutor;
import io.astradb.queryexecutor.model.ExecutionResult;
import java.util.Objects;

/**
 * Coordinates query plans and delegates their execution.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class QueryCoordinator<K, V> {

  private final QueryExecutor<K, V> queryExecutor;

  public QueryCoordinator(QueryExecutor<K, V> queryExecutor) {
    this.queryExecutor = Objects.requireNonNull(queryExecutor, "Query executor must not be null");
  }

  /**
   * Coordinates and executes a query plan.
   *
   * @param queryPlan query plan to execute
   * @return correlated execution response
   */
  public QueryExecutionResponse<V> execute(QueryPlan<K, V> queryPlan) {
    Objects.requireNonNull(queryPlan, "Query plan must not be null");

    try {
      ExecutionResult<V> result = queryExecutor.execute(queryPlan.getExecutionRequest());

      return new QueryExecutionResponse<>(queryPlan.getRequestId(), result);
    } catch (QueryCoordinatorException exception) {
      throw exception;
    } catch (RuntimeException exception) {
      throw new QueryCoordinatorException(
          "Failed to coordinate query request: " + queryPlan.getRequestId(), exception);
    }
  }
}
