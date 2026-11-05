package io.astradb.storage.rocksdb;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class StringValueSerializerTests {

  private final StringValueSerializer serializer = new StringValueSerializer();

  @Test
  void shouldSerializeStringToUtf8() {
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), serializer.serialize("hello"));
  }

  @Test
  void shouldDeserializeUtf8ToString() {
    assertEquals("hello", serializer.deserialize("hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void shouldSupportUnicodeCharacters() {
    String value = "AstraDB 数据库 ";

    assertEquals(value, serializer.deserialize(serializer.serialize(value)));
  }

  @Test
  void shouldRejectNullValue() {
    assertThrows(NullPointerException.class, () -> serializer.serialize(null));
  }

  @Test
  void shouldRejectNullBytes() {
    assertThrows(NullPointerException.class, () -> serializer.deserialize(null));
  }
}
