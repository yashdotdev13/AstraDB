package io.astradb.raft.election;

/** Configuration for Raft leader elections. */
public final class RaftElectionConfig {

  private final long minimumElectionTimeoutMillis;
  private final long maximumElectionTimeoutMillis;

  public RaftElectionConfig(long minimumElectionTimeoutMillis, long maximumElectionTimeoutMillis) {

    if (minimumElectionTimeoutMillis <= 0) {
      throw new IllegalArgumentException("Minimum election timeout must be positive");
    }

    if (maximumElectionTimeoutMillis < minimumElectionTimeoutMillis) {
      throw new IllegalArgumentException(
          "Maximum election timeout must be greater than or equal to minimum timeout");
    }

    if (maximumElectionTimeoutMillis == Long.MAX_VALUE) {
      throw new IllegalArgumentException("Maximum election timeout is too large");
    }

    this.minimumElectionTimeoutMillis = minimumElectionTimeoutMillis;
    this.maximumElectionTimeoutMillis = maximumElectionTimeoutMillis;
  }

  public long getMinimumElectionTimeoutMillis() {
    return minimumElectionTimeoutMillis;
  }

  public long getMaximumElectionTimeoutMillis() {
    return maximumElectionTimeoutMillis;
  }
}
