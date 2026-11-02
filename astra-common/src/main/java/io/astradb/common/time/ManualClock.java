package io.astradb.common.time;

/**
 * Controllable clock for deterministic time-dependent operations.
 *
 * <p>Useful for simulations and tests that need to advance or adjust time without waiting for the
 * real system clock.
 */
public final class ManualClock implements Clock {

  private long currentTimeMillis;

  /**
   * Creates a manual clock initialized to the specified time.
   *
   * @param initialTimeMillis initial time in milliseconds
   */
  public ManualClock(long initialTimeMillis) {
    if (initialTimeMillis < 0) {
      throw new IllegalArgumentException("Initial time cannot be negative");
    }

    this.currentTimeMillis = initialTimeMillis;
  }

  @Override
  public synchronized long currentTimeMillis() {
    return currentTimeMillis;
  }

  /**
   * Sets the current time.
   *
   * @param newTimeMillis new time in milliseconds
   */
  public synchronized void setTimeMillis(long newTimeMillis) {
    if (newTimeMillis < 0) {
      throw new IllegalArgumentException("Time cannot be negative");
    }

    this.currentTimeMillis = newTimeMillis;
  }

  /**
   * Advances the clock by the specified duration.
   *
   * @param millis duration to advance in milliseconds
   */
  public synchronized void advanceMillis(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("Advance duration cannot be negative");
    }

    currentTimeMillis = Math.addExact(currentTimeMillis, millis);
  }
}
