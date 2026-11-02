package io.astradb.common.time;

/**
 * Abstraction for retrieving the current physical time.
 *
 * <p>Allows time-dependent components to use injectable time sources for deterministic testing and
 * simulation.
 */
@FunctionalInterface
public interface Clock {

  /**
   * Returns the current physical time in milliseconds.
   *
   * @return current time in milliseconds
   */
  long currentTimeMillis();
}
