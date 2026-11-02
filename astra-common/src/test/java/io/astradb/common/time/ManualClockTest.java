package io.astradb.common.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ManualClockTest {

  @Test
  void shouldInitializeWithProvidedTime() {
    ManualClock clock = new ManualClock(1000L);

    assertEquals(1000L, clock.currentTimeMillis());
  }

  @Test
  void shouldSetTime() {
    ManualClock clock = new ManualClock(1000L);

    clock.setTimeMillis(2000L);

    assertEquals(2000L, clock.currentTimeMillis());
  }

  @Test
  void shouldAdvanceTime() {
    ManualClock clock = new ManualClock(1000L);

    clock.advanceMillis(500L);

    assertEquals(1500L, clock.currentTimeMillis());
  }

  @Test
  void shouldAllowZeroInitialTime() {
    ManualClock clock = new ManualClock(0L);

    assertEquals(0L, clock.currentTimeMillis());
  }

  @Test
  void shouldRejectNegativeInitialTime() {
    assertThrows(IllegalArgumentException.class, () -> new ManualClock(-1L));
  }

  @Test
  void shouldRejectNegativeSetTime() {
    ManualClock clock = new ManualClock(1000L);

    assertThrows(IllegalArgumentException.class, () -> clock.setTimeMillis(-1L));
  }

  @Test
  void shouldRejectNegativeAdvance() {
    ManualClock clock = new ManualClock(1000L);

    assertThrows(IllegalArgumentException.class, () -> clock.advanceMillis(-1L));
  }

  @Test
  void shouldDetectTimeOverflowWhenAdvancing() {
    ManualClock clock = new ManualClock(Long.MAX_VALUE);

    assertThrows(ArithmeticException.class, () -> clock.advanceMillis(1L));
  }
}
