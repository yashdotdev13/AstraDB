package io.astradb.raft.log;

/** Indicates a failure while accessing the persistent Raft log. */
public class RaftLogException extends RuntimeException {

  public RaftLogException(String message) {
    super(message);
  }

  public RaftLogException(String message, Throwable cause) {
    super(message, cause);
  }
}
