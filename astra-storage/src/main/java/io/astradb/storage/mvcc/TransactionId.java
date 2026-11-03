package io.astradb.storage.mvcc;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents the unique identity of an MVCC transaction.
 *
 * <p>A TransactionId is immutable and suitable for identifying transactions across storage
 * operations, concurrency control, and distributed transaction coordination.
 */
public final class TransactionId {

  private final UUID value;

  private TransactionId(UUID value) {
    this.value = Objects.requireNonNull(value, "TransactionId value must not be null");
  }

  /**
   * Creates a new randomly generated transaction identity.
   *
   * @return a new unique TransactionId
   */
  public static TransactionId generate() {
    return new TransactionId(UUID.randomUUID());
  }

  /**
   * Creates a TransactionId from an existing UUID.
   *
   * @param value UUID representing the transaction identity
   * @return TransactionId instance
   */
  public static TransactionId from(UUID value) {
    return new TransactionId(value);
  }

  /**
   * Parses a TransactionId from its string representation.
   *
   * @param value string representation of a UUID
   * @return parsed TransactionId
   * @throws IllegalArgumentException if the value is not a valid UUID
   */
  public static TransactionId parse(String value) {
    Objects.requireNonNull(value, "TransactionId string must not be null");
    return new TransactionId(UUID.fromString(value));
  }

  /**
   * Returns the underlying UUID.
   *
   * @return UUID value
   */
  public UUID getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TransactionId)) {
      return false;
    }
    TransactionId other = (TransactionId) obj;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
