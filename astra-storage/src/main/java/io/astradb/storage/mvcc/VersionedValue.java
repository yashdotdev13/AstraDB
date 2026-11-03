package io.astradb.storage.mvcc;

import io.astradb.common.time.HybridTimestamp;
import java.util.Objects;

/**
 * Represents an immutable version of a value in the MVCC storage engine.
 *
 * <p>A version is identified by its hybrid timestamp. It may contain a regular value or represent a
 * deletion through a tombstone.
 *
 * @param <V> type of the stored value
 */
public final class VersionedValue<V> {

  private final HybridTimestamp timestamp;
  private final V value;
  private final boolean tombstone;

  private VersionedValue(HybridTimestamp timestamp, V value, boolean tombstone) {
    this.timestamp = Objects.requireNonNull(timestamp, "Timestamp must not be null");

    if (tombstone && value != null) {
      throw new IllegalArgumentException("A tombstone must not contain a value");
    }

    if (!tombstone && value == null) {
      throw new NullPointerException("A non-tombstone version must contain a value");
    }

    this.value = value;
    this.tombstone = tombstone;
  }

  /**
   * Creates a regular value version.
   *
   * @param timestamp timestamp identifying this version
   * @param value stored value
   * @param <V> type of the stored value
   * @return a value version
   */
  public static <V> VersionedValue<V> of(HybridTimestamp timestamp, V value) {
    return new VersionedValue<>(timestamp, value, false);
  }

  /**
   * Creates a tombstone version representing a deletion.
   *
   * @param timestamp timestamp identifying the deletion
   * @param <V> type of the stored value
   * @return a tombstone version
   */
  public static <V> VersionedValue<V> tombstone(HybridTimestamp timestamp) {
    return new VersionedValue<>(timestamp, null, true);
  }

  public HybridTimestamp getTimestamp() {
    return timestamp;
  }

  public V getValue() {
    return value;
  }

  public boolean isTombstone() {
    return tombstone;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof VersionedValue<?>)) {
      return false;
    }
    VersionedValue<?> other = (VersionedValue<?>) obj;
    return timestamp.equals(other.timestamp)
        && Objects.equals(value, other.value)
        && tombstone == other.tombstone;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, value, tombstone);
  }

  @Override
  public String toString() {
    return "VersionedValue{"
        + "timestamp="
        + timestamp
        + ", value="
        + value
        + ", tombstone="
        + tombstone
        + '}';
  }
}
