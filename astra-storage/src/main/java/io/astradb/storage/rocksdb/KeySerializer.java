package io.astradb.storage.rocksdb;

/**
 * Serializes and deserializes keys for persistent storage.
 *
 * @param <K> key type
 */
public interface KeySerializer<K> {

  /**
   * Serializes a key into bytes.
   *
   * @param key key to serialize
   * @return serialized key
   */
  byte[] serialize(K key);

  /**
   * Deserializes bytes into a key.
   *
   * @param bytes serialized key
   * @return deserialized key
   */
  K deserialize(byte[] bytes);
}
