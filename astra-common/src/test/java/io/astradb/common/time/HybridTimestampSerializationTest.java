package io.astradb.common.time;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class HybridTimestampSerializationTest {

  @Test
  void shouldRoundTripTimestamp() {
    HybridTimestamp original = new HybridTimestamp(1_700_000_000_000L, 42L);

    HybridTimestamp restored = HybridTimestamp.fromBytes(original.toBytes());

    assertEquals(original, restored);
  }

  @Test
  void shouldRoundTripZeroTimestamp() {
    HybridTimestamp original = new HybridTimestamp(0L, 0L);

    HybridTimestamp restored = HybridTimestamp.fromBytes(original.toBytes());

    assertEquals(original, restored);
  }

  @Test
  void shouldRoundTripMaximumLogicalCounter() {
    HybridTimestamp original = new HybridTimestamp(123L, Long.MAX_VALUE);

    HybridTimestamp restored = HybridTimestamp.fromBytes(original.toBytes());

    assertEquals(original, restored);
  }

  @Test
  void shouldPreserveLargePhysicalTime() {
    HybridTimestamp original = new HybridTimestamp(Long.MAX_VALUE, 100L);

    HybridTimestamp restored = HybridTimestamp.fromBytes(original.toBytes());

    assertEquals(original, restored);
  }

  @Test
  void shouldSerializeUsingBigEndianFormat() {
    HybridTimestamp timestamp = new HybridTimestamp(0x0102030405060708L, 0x1112131415161718L);

    byte[] expected =
        ByteBuffer.allocate(16)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(0x0102030405060708L)
            .putLong(0x1112131415161718L)
            .array();

    assertArrayEquals(expected, timestamp.toBytes());
  }

  @Test
  void shouldAlwaysProduceSixteenBytes() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 10L);

    assertEquals(16, timestamp.toBytes().length);
  }

  @Test
  void shouldRejectNullBytes() {
    assertThrows(NullPointerException.class, () -> HybridTimestamp.fromBytes(null));
  }

  @Test
  void shouldRejectEmptyByteArray() {
    assertThrows(IllegalArgumentException.class, () -> HybridTimestamp.fromBytes(new byte[0]));
  }

  @Test
  void shouldRejectShortByteArray() {
    assertThrows(IllegalArgumentException.class, () -> HybridTimestamp.fromBytes(new byte[15]));
  }

  @Test
  void shouldRejectLongByteArray() {
    assertThrows(IllegalArgumentException.class, () -> HybridTimestamp.fromBytes(new byte[17]));
  }

  @Test
  void shouldRejectNegativePhysicalTime() {
    byte[] bytes =
        ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN).putLong(-1L).putLong(0L).array();

    assertThrows(IllegalArgumentException.class, () -> HybridTimestamp.fromBytes(bytes));
  }

  @Test
  void shouldRejectNegativeLogicalCounter() {
    byte[] bytes =
        ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN).putLong(0L).putLong(-1L).array();

    assertThrows(IllegalArgumentException.class, () -> HybridTimestamp.fromBytes(bytes));
  }
}
