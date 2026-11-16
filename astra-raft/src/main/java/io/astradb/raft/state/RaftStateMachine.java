package io.astradb.raft.state;

/**
 * Applies committed Raft log entries to the application state.
 *
 * <p>Implementations should process each index atomically and use the index to make retries safe if
 * application fails before lastApplied advances.
 */
@FunctionalInterface
public interface RaftStateMachine {

  /**
   * Applies a committed command.
   *
   * @param index the log index of the command
   * @param command the command payload
   */
  void apply(long index, byte[] command);

  /**
   * Restores the application state from a Raft snapshot.
   *
   * <p>Implementations that support snapshot installation must override this method.
   *
   * @param snapshotData serialized snapshot data
   * @throws UnsupportedOperationException if snapshot restoration is not supported
   */
  default void restore(byte[] snapshotData) {
    throw new UnsupportedOperationException(
        "Snapshot restoration is not supported by this state machine");
  }
}
