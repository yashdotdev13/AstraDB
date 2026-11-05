package io.astradb.storage.rocksdb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Serializes String keys using UTF-8. */
public final class StringKeySerializer implements KeySerializer<String> {

  @Override
  public byte[] serialize(String key) {
    Objects.requireNonNull(key, "Key must not be null");
    return key.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String deserialize(byte[] bytes) {
    Objects.requireNonNull(bytes, "Serialized key must not be null");
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
