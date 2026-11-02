package io.astradb.common.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class HybridTimestampOracleTest {

  @Test
  void shouldCreateOracleUsingSystemClock() {
    TimestampOracle oracle = new HybridTimestampOracle();

    assertNotNull(oracle.now());
  }

  @Test
  void shouldCreateOracleUsingInjectedClock() {
    ManualClock clock = new ManualClock(1000);
    TimestampOracle oracle = new HybridTimestampOracle(clock);

    assertEquals(new HybridTimestamp(1000, 0), oracle.peek());
  }

  @Test
  void shouldRejectNullClock() {
    assertThrows(NullPointerException.class, () -> new HybridTimestampOracle(null));
  }

  @Test
  void shouldGenerateIncreasingTimestamps() {
    TimestampOracle oracle = new HybridTimestampOracle(new ManualClock(1000));

    HybridTimestamp first = oracle.now();
    HybridTimestamp second = oracle.now();

    assertEquals(true, first.compareTo(second) < 0);
  }

  @Test
  void shouldUpdateTimestampFromRemoteEvent() {
    TimestampOracle oracle = new HybridTimestampOracle(new ManualClock(1000));

    HybridTimestamp remote = new HybridTimestamp(2000, 5);
    HybridTimestamp updated = oracle.update(remote);

    assertEquals(new HybridTimestamp(2000, 6), updated);
  }

  @Test
  void shouldPeekWithoutAdvancingTimestamp() {
    TimestampOracle oracle = new HybridTimestampOracle(new ManualClock(1000));

    HybridTimestamp before = oracle.peek();
    HybridTimestamp after = oracle.peek();

    assertEquals(before, after);
  }

  @Test
  void shouldRejectNullRemoteTimestamp() {
    TimestampOracle oracle = new HybridTimestampOracle(new ManualClock(1000));

    assertThrows(NullPointerException.class, () -> oracle.update(null));
  }
}
