package io.astradb.transaction.mutation;

import java.util.Objects;

/** Represents a mutation staged by an active transaction. */
public final class PendingMutation<K, V> {

  /** Supported mutation operations. */
  public enum Type {
    PUT,
    DELETE
  }

  private final Type type;
  private final K key;
  private final V value;

  private PendingMutation(Type type, K key, V value) {
    this.type = Objects.requireNonNull(type, "Mutation type must not be null");
    this.key = Objects.requireNonNull(key, "Mutation key must not be null");

    if (type == Type.PUT) {
      this.value = Objects.requireNonNull(value, "Mutation value must not be null");
    } else {
      this.value = null;
    }
  }

  public static <K, V> PendingMutation<K, V> put(K key, V value) {
    return new PendingMutation<>(Type.PUT, key, value);
  }

  public static <K, V> PendingMutation<K, V> delete(K key) {
    return new PendingMutation<>(Type.DELETE, key, null);
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

  public boolean isDelete() {
    return type == Type.DELETE;
  }
}
