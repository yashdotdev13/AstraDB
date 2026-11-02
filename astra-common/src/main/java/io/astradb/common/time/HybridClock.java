package io.astradb.common.time;

import java.util.Objects;

/**
 * Thread-safe hybrid logical clock for generating monotonically increasing timestamps.
 *
 * <p>Combines physical time with a logical counter to maintain event ordering in distributed
 * systems.
 */
public final class HybridClock {

  private final Clock clock;

  private long physicalTime;
  private long logicalCounter;

  /** Creates a hybrid clock backed by the system wall-clock time. */
  public HybridClock() {
    this(new SystemClock());
  }

  /**
   * Creates a hybrid clock using the specified time source.
   *
   * @param clock time source
   */
  public HybridClock(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "Clock must not be null");
    long initialTime = clock.currentTimeMillis();

    if (initialTime < 0) {
      throw new IllegalArgumentException("Clock time cannot be negative");
    }

    this.physicalTime = initialTime;
    this.logicalCounter = 0;
  }

  /**
   * Generates the next monotonically increasing hybrid timestamp.
   *
   * @return next hybrid timestamp
   */
  public synchronized HybridTimestamp now() {
    long currentTime = clock.currentTimeMillis();

    if (currentTime > physicalTime) {
      physicalTime = currentTime;
      logicalCounter = 0;
    } else {
      incrementLogicalCounter();
    }

    return new HybridTimestamp(physicalTime, logicalCounter);
  }

  /**
   * Returns the current clock timestamp without modifying its state.
   *
   * @return current hybrid timestamp
   */
  public synchronized HybridTimestamp peek() {
    return new HybridTimestamp(physicalTime, logicalCounter);
  }

  /**
   * Updates the local clock using a timestamp received from another node.
   *
   * @param remoteTimestamp timestamp received from another node
   */
  public synchronized HybridTimestamp update(HybridTimestamp remoteTimestamp) {
    Objects.requireNonNull(remoteTimestamp, "remoteTimestamp must not be null");

    long currentTime = clock.currentTimeMillis();
    long remotePhysicalTime = remoteTimestamp.getPhysicalTime();
    long remoteLogicalCounter = remoteTimestamp.getLogicalCounter();

    long maxPhysicalTime = Math.max(currentTime, Math.max(physicalTime, remotePhysicalTime));

    if (maxPhysicalTime == physicalTime && maxPhysicalTime == remotePhysicalTime) {
      logicalCounter = Math.max(logicalCounter, remoteLogicalCounter);
      incrementLogicalCounter();
    } else if (maxPhysicalTime == physicalTime) {
      incrementLogicalCounter();
    } else if (maxPhysicalTime == remotePhysicalTime) {
      physicalTime = remotePhysicalTime;
      logicalCounter = remoteLogicalCounter;

      incrementLogicalCounter();
    } else {
      physicalTime = currentTime;
      logicalCounter = 0;
    }

    return new HybridTimestamp(physicalTime, logicalCounter);
  }

  private void incrementLogicalCounter() {
    if (logicalCounter == Long.MAX_VALUE) {
      physicalTime = Math.incrementExact(physicalTime);
      logicalCounter = 0;
    } else {
      logicalCounter++;
    }
  }
}
