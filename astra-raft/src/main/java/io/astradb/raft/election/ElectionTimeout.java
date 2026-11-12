package io.astradb.raft.election;

import java.util.Objects;
import java.util.random.RandomGenerator;

/** Generates randomized election timeouts to reduce simultaneous elections. */
public final class ElectionTimeout {

  private final RaftElectionConfig config;
  private final RandomGenerator randomGenerator;

  public ElectionTimeout(RaftElectionConfig config, RandomGenerator randomGenerator) {
    this.config = Objects.requireNonNull(config, "Election config must not be null");
    this.randomGenerator =
        Objects.requireNonNull(randomGenerator, "Random generator must not be null");
  }

  /** Returns a random timeout in the configured inclusive range. */
  public long nextTimeoutMillis() {
    long minimum = config.getMinimumElectionTimeoutMillis();
    long maximum = config.getMaximumElectionTimeoutMillis();

    if (minimum == maximum) {
      return minimum;
    }

    return randomGenerator.nextLong(minimum, maximum + 1);
  }
}
