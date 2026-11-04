package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LogSequenceNumberTest {

  @Test
  void shouldCreateLogSequenceNumber() {
    LogSequenceNumber lsn = LogSequenceNumber.of(10L);

    assertEquals(10L, lsn.getValue());
  }

  @Test
  void shouldAllowZeroAsInitialSequenceNumber() {
    LogSequenceNumber lsn = LogSequenceNumber.of(0L);

    assertEquals(0L, lsn.getValue());
  }

  @Test
  void shouldRejectNegativeSequenceNumber() {
    assertThrows(IllegalArgumentException.class, () -> LogSequenceNumber.of(-1L));
  }

  @Test
  void shouldCompareSequenceNumbers() {
    LogSequenceNumber first = LogSequenceNumber.of(1L);
    LogSequenceNumber second = LogSequenceNumber.of(2L);
    LogSequenceNumber third = LogSequenceNumber.of(3L);

    assertTrue(first.compareTo(second) < 0);
    assertTrue(second.compareTo(first) > 0);
    assertTrue(first.compareTo(third) < 0);
  }

  @Test
  void shouldCompareEqualSequenceNumbers() {
    LogSequenceNumber first = LogSequenceNumber.of(10L);
    LogSequenceNumber second = LogSequenceNumber.of(10L);

    assertEquals(0, first.compareTo(second));
    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void shouldRejectNullWhenComparing() {
    LogSequenceNumber lsn = LogSequenceNumber.of(10L);

    assertThrows(NullPointerException.class, () -> lsn.compareTo(null));
  }

  @Test
  void shouldImplementToString() {
    LogSequenceNumber lsn = LogSequenceNumber.of(42L);

    assertEquals("LSN{42}", lsn.toString());
  }
}
