package io.astradb.storage.rocksdb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Serializes String values using UTF-8. */
public final class StringValueSerializer implements ValueSerializer<String> {

  @Override
  public byte[] serialize(String value) {
    Objects.requireNonNull(value, "Value must not be null");
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String deserialize(byte[] bytes) {
    Objects.requireNonNull(bytes, "Serialized value must not be null");
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
