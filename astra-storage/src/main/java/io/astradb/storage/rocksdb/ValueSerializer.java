package io.astradb.storage.rocksdb;

/**
 * Serializes and deserializes values for persistent storage.
 *
 * @param <V> value type
 */
public interface ValueSerializer<V> {

  /**
   * Serializes a value into bytes.
   *
   * @param value value to serialize
   * @return serialized value
   */
  byte[] serialize(V value);

  /**
   * Deserializes bytes into a value.
   *
   * @param bytes serialized value
   * @return deserialized value
   */
  V deserialize(byte[] bytes);
}
