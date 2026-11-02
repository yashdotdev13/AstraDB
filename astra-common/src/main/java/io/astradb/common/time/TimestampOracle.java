package io.astradb.common.time;

/**
 * Provides timestamps for ordering events in a distributed system.
 *
 * <p>Implementations should provide timestamps that preserve causal ordering across events observed
 * by the system.
 */
public interface TimestampOracle {

  /**
   * Returns a timestamp representing a new event.
   *
   * @return a new hybrid timestamp
   */
  HybridTimestamp now();

  /**
   * Updates the local timestamp based on a timestamp received from another node and returns a
   * timestamp representing the subsequent local event.
   *
   * @param remoteTimestamp the timestamp received from another node
   * @return a new hybrid timestamp that preserves causal ordering
   */
  HybridTimestamp update(HybridTimestamp remoteTimestamp);

  /**
   * Returns the current timestamp without creating a new event.
   *
   * @return the current hybrid timestamp
   */
  HybridTimestamp peek();
}
