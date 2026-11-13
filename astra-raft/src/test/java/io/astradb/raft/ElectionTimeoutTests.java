package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.election.ElectionTimeout;
import io.astradb.raft.election.RaftElectionConfig;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class ElectionTimeoutTests {

  @Test
  void shouldGenerateTimeoutWithinConfiguredRange() {
    RaftElectionConfig config = new RaftElectionConfig(150, 300);
    ElectionTimeout timeout = new ElectionTimeout(config, RandomGenerator.getDefault());

    for (int i = 0; i < 1_000; i++) {
      long value = timeout.nextTimeoutMillis();

      assertTrue(value >= 150);
      assertTrue(value <= 300);
    }
  }

  @Test
  void shouldReturnMinimumWhenBoundsAreEqual() {
    RaftElectionConfig config = new RaftElectionConfig(200, 200);
    ElectionTimeout timeout = new ElectionTimeout(config, RandomGenerator.getDefault());

    assertEquals(200, timeout.nextTimeoutMillis());
  }

  @Test
  void shouldAllowMaximumTimeoutValue() {
    RaftElectionConfig config = new RaftElectionConfig(100, 500);
    RandomGenerator generator =
        new RandomGenerator() {
          @Override
          public long nextLong() {
            return 0;
          }

          @Override
          public long nextLong(long origin, long bound) {
            return bound - 1;
          }
        };

    ElectionTimeout timeout = new ElectionTimeout(config, generator);

    assertEquals(500, timeout.nextTimeoutMillis());
  }

  @Test
  void shouldRejectInvalidTimeoutBounds() {
    assertThrows(IllegalArgumentException.class, () -> new RaftElectionConfig(0, 300));
    assertThrows(IllegalArgumentException.class, () -> new RaftElectionConfig(-1, 300));
    assertThrows(IllegalArgumentException.class, () -> new RaftElectionConfig(300, 200));
    assertThrows(IllegalArgumentException.class, () -> new RaftElectionConfig(100, Long.MAX_VALUE));
  }

  @Test
  void shouldRejectNullDependencies() {
    RaftElectionConfig config = new RaftElectionConfig(100, 300);

    assertThrows(
        NullPointerException.class, () -> new ElectionTimeout(null, RandomGenerator.getDefault()));

    assertThrows(NullPointerException.class, () -> new ElectionTimeout(config, null));
  }
}
