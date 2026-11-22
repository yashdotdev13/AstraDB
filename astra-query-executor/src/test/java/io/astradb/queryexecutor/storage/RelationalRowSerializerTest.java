package io.astradb.queryexecutor.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RelationalRowSerializerTest {

  private RelationalRowSerializer serializer;

  @BeforeEach
  void setUp() {
    serializer = new RelationalRowSerializer();
  }

  @Test
  void shouldSerializeAndDeserializeBasicValues() {
    RelationalRow original = RelationalRow.of(101, "Yash", 21L, true, 85.5, null);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldPreserveCompleteSupportedValueTypes() {
    UUID uuid = UUID.randomUUID();

    RelationalRow original =
        RelationalRow.of(
            (byte) 10,
            (short) 200,
            1000,
            50000L,
            12.5f,
            99.99d,
            new BigDecimal("123456789.123456789"),
            "ASTRA-DB",
            true,
            LocalDate.of(2026, 11, 21),
            LocalTime.of(23, 30, 45),
            LocalDateTime.of(2026, 11, 21, 23, 30, 45),
            uuid);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldPreserveNullValues() {
    RelationalRow original = RelationalRow.of(null, "ASTRA", null, 100, null);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldPreserveEmptyStrings() {
    RelationalRow original = RelationalRow.of("", "ASTRA-DB", "");

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldPreserveBinaryValues() {
    byte[] binaryValue = {0, 1, 2, 3, 127, -1};

    RelationalRow original = RelationalRow.of(binaryValue);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertArrayEquals(binaryValue, (byte[]) restored.getValue(0));
  }

  @Test
  void shouldPreserveMultipleBinaryValues() {
    byte[] first = {1, 2, 3};
    byte[] second = {10, 20, 30};

    RelationalRow original = RelationalRow.of(first, second);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertArrayEquals(first, (byte[]) restored.getValue(0));
    assertArrayEquals(second, (byte[]) restored.getValue(1));
  }

  @Test
  void shouldPreserveHighPrecisionDecimalValues() {
    BigDecimal decimal = new BigDecimal("999999999999999999.1234567890123456789");

    RelationalRow original = RelationalRow.of(decimal);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
    assertEquals(decimal, restored.getValue(0));
  }

  @Test
  void shouldPreserveUuidValues() {
    UUID uuid = UUID.randomUUID();

    RelationalRow original = RelationalRow.of(uuid);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(uuid, restored.getValue(0));
  }

  @Test
  void shouldPreserveTemporalValues() {
    RelationalRow original =
        RelationalRow.of(
            LocalDate.of(2025, 1, 15),
            LocalTime.of(10, 30, 45, 123456789),
            LocalDateTime.of(2025, 1, 15, 10, 30, 45, 123456789));

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldHandleEmptyRow() {
    RelationalRow original = RelationalRow.of();

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldProduceNonNullSerializedData() {
    RelationalRow original = RelationalRow.of(1, "ASTRA");

    byte[] serialized = serializer.serialize(original);

    assertNotNull(serialized);
    assertTrue(serialized.length > 0);
  }

  @Test
  void shouldReturnNullForNullValue() {
    RelationalRow original = RelationalRow.of((Object) null);

    RelationalRow restored = serializer.deserialize(serializer.serialize(original));

    assertNull(restored.getValue(0));
  }

  @Test
  void shouldRejectNullSerializedData() {
    assertThrows(RuntimeException.class, () -> serializer.deserialize(null));
  }

  @Test
  void shouldRejectInvalidSerializedData() {
    byte[] invalidData = {0, 1, 2, 3, 4, 5};

    assertThrows(RuntimeException.class, () -> serializer.deserialize(invalidData));
  }
}
