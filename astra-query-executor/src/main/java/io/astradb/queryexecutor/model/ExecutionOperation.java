package io.astradb.queryexecutor.model;

/** Defines the operations supported by the query execution layer. */
public enum ExecutionOperation {
  BEGIN,
  GET,
  PUT,
  DELETE,
  COMMIT,
  ROLLBACK
}
