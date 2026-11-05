package io.astradb.storage.rocksdb;

import java.util.Objects;

/**
 * Represents a single mutation within a storage batch.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class BatchOperation<K, V> {

  public enum Type {
    PUT,
    DELETE
  }

  private final Type type;
  private final K key;
  private final V value;

  private BatchOperation(Type type, K key, V value) {
    this.type = Objects.requireNonNull(type, "Operation type must not be null");
    this.key = Objects.requireNonNull(key, "Key must not be null");

    if (type == Type.PUT) {
      this.value = Objects.requireNonNull(value, "Value must not be null for PUT");
    } else {
      this.value = null;
    }
  }

  public static <K, V> BatchOperation<K, V> put(K key, V value) {
    return new BatchOperation<>(Type.PUT, key, value);
  }

  public static <K, V> BatchOperation<K, V> delete(K key) {
    return new BatchOperation<>(Type.DELETE, key, null);
  }

  public Type getType() {
    return type;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }
}
