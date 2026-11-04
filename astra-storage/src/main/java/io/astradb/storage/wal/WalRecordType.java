package io.astradb.storage.wal;

/** Defines the types of records supported by the write-ahead log. */
public enum WalRecordType {

  /** Marks the beginning of a transaction. */
  BEGIN,

  /** Represents an insert or update operation. */
  PUT,

  /** Represents a key deletion operation. */
  DELETE,

  /** Marks a successfully committed transaction. */
  COMMIT,

  /** Marks an aborted transaction. */
  ABORT
}
