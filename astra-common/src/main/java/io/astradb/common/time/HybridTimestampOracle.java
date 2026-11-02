package io.astradb.common.time;

import java.util.Objects;

/** Default {@link TimestampOracle} implementation backed by a {@link HybridClock}. */
public final class HybridTimestampOracle implements TimestampOracle {

  private final HybridClock hybridClock;

  /** Creates an oracle backed by a system clock. */
  public HybridTimestampOracle() {
    this.hybridClock = new HybridClock();
  }

  /**
   * Creates an oracle backed by the provided clock.
   *
   * @param clock the clock used by the underlying hybrid clock
   */
  public HybridTimestampOracle(Clock clock) {
    this.hybridClock = new HybridClock(Objects.requireNonNull(clock, "clock must not be null"));
  }

  @Override
  public HybridTimestamp now() {
    return hybridClock.now();
  }

  @Override
  public HybridTimestamp update(HybridTimestamp remoteTimestamp) {
    return hybridClock.update(remoteTimestamp);
  }

  @Override
  public HybridTimestamp peek() {
    return hybridClock.peek();
  }
}
