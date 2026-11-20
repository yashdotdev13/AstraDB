package io.astradb.queryexecutor.execution;

import io.astradb.common.identity.RequestId;
import io.astradb.storage.mvcc.TransactionId;
import java.util.Objects;

/** Carries correlation and transaction information during query execution. */
public final class ExecutionContext {

  private final RequestId requestId;
  private final TransactionId transactionId;

  public ExecutionContext(RequestId requestId, TransactionId transactionId) {
    this.requestId = Objects.requireNonNull(requestId, "Request ID must not be null");
    this.transactionId = transactionId;
  }

  public RequestId getRequestId() {
    return requestId;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }
}
