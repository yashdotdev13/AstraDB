package io.astradb.common.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HybridTimestampTest {

  @Test
  void shouldCreateTimestamp() {
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 5);

    assertEquals(1000L, timestamp.getPhysicalTime());
    assertEquals(5, timestamp.getLogicalCounter());
  }

  @Test
  void shouldAllowZeroValues() {
    HybridTimestamp timestamp = new HybridTimestamp(0L, 0);

    assertEquals(0L, timestamp.getPhysicalTime());
    assertEquals(0, timestamp.getLogicalCounter());
  }

  @Test
  void shouldRejectNegativePhysicalTime() {
    assertThrows(IllegalArgumentException.class, () -> new HybridTimestamp(-1L, 0));
  }

  @Test
  void shouldRejectNegativeLogicalCounter() {
    assertThrows(IllegalArgumentException.class, () -> new HybridTimestamp(1000L, -1));
  }

  @Test
  void shouldComparePhysicalTime() {
    HybridTimestamp earlier = new HybridTimestamp(1000L, 10);
    HybridTimestamp later = new HybridTimestamp(2000L, 0);

    assertTrue(earlier.compareTo(later) < 0);
    assertTrue(later.compareTo(earlier) > 0);
  }

  @Test
  void shouldCompareLogicalCounterWhenPhysicalTimeIsEqual() {
    HybridTimestamp earlier = new HybridTimestamp(1000L, 1);
    HybridTimestamp later = new HybridTimestamp(1000L, 2);

    assertTrue(earlier.compareTo(later) < 0);
    assertTrue(later.compareTo(earlier) > 0);
  }

  @Test
  void shouldReturnZeroWhenTimestampsAreEqual() {
    HybridTimestamp first = new HybridTimestamp(1000L, 5);
    HybridTimestamp second = new HybridTimestamp(1000L, 5);

    assertEquals(0, first.compareTo(second));
  }

  @Test
  void shouldRejectNullInComparison() {
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 5);

    assertThrows(NullPointerException.class, () -> timestamp.compareTo(null));
  }

  @Test
  void shouldBeEqualWhenComponentsAreEqual() {
    HybridTimestamp first = new HybridTimestamp(1000L, 5);
    HybridTimestamp second = new HybridTimestamp(1000L, 5);

    assertEquals(first, second);
  }

  @Test
  void shouldNotBeEqualWhenPhysicalTimeDiffers() {
    HybridTimestamp first = new HybridTimestamp(1000L, 5);
    HybridTimestamp second = new HybridTimestamp(2000L, 5);

    assertNotEquals(first, second);
  }

  @Test
  void shouldNotBeEqualWhenLogicalCounterDiffers() {
    HybridTimestamp first = new HybridTimestamp(1000L, 5);
    HybridTimestamp second = new HybridTimestamp(1000L, 6);

    assertNotEquals(first, second);
  }

  @Test
  void shouldMaintainHashCodeContract() {
    HybridTimestamp first = new HybridTimestamp(1000L, 5);
    HybridTimestamp second = new HybridTimestamp(1000L, 5);

    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void shouldGenerateExpectedStringRepresentation() {
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 5);

    assertEquals("1000:5", timestamp.toString());
  }

  @Test
  void shouldSupportLogicalCountersBeyondIntegerMaxValue() {
    long largeCounter = (long) Integer.MAX_VALUE + 1;

    HybridTimestamp timestamp = new HybridTimestamp(1000L, largeCounter);
    HybridTimestamp smallerTimestamp = new HybridTimestamp(1000L, Integer.MAX_VALUE);

    assertEquals(largeCounter, timestamp.getLogicalCounter());
    assertTrue(timestamp.compareTo(smallerTimestamp) > 0);
  }
}
