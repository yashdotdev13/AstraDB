package io.astradb.common.time;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Represents a hybrid logical timestamp composed of physical time and a logical counter.
 *
 * <p>Hybrid timestamps provide a deterministic ordering of events in distributed systems while
 * retaining a close relationship with wall-clock time.
 */
public final class HybridTimestamp implements Comparable<HybridTimestamp> {

  private final long physicalTime;
  private final long logicalCounter;

  /**
   * Creates a hybrid timestamp.
   *
   * @param physicalTime physical time in milliseconds
   * @param logicalCounter logical counter for ordering events
   */
  public HybridTimestamp(long physicalTime, long logicalCounter) {
    if (physicalTime < 0) {
      throw new IllegalArgumentException("Physical time cannot be negative");
    }
    if (logicalCounter < 0) {
      throw new IllegalArgumentException("Logical counter cannot be negative");
    }

    this.physicalTime = physicalTime;
    this.logicalCounter = logicalCounter;
  }

  public long getPhysicalTime() {
    return physicalTime;
  }

  public long getLogicalCounter() {
    return logicalCounter;
  }

  /**
   * Serializes this timestamp into a 16-byte big-endian representation.
   *
   * <p>The first 8 bytes represent the physical time in milliseconds. The remaining 8 bytes
   * represent the logical counter.
   *
   * @return serialized timestamp bytes
   */
  public byte[] toBytes() {
    return ByteBuffer.allocate(Long.BYTES * 2)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(physicalTime)
        .putLong(logicalCounter)
        .array();
  }

  /**
   * Deserializes a timestamp from its 16-byte big-endian representation.
   *
   * @param bytes serialized timestamp bytes
   * @return reconstructed hybrid timestamp
   * @throws NullPointerException if bytes is null
   * @throws IllegalArgumentException if the byte array length is invalid or contains negative
   *     values
   */
  public static HybridTimestamp fromBytes(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes must not be null");

    if (bytes.length != Long.BYTES * 2) {
      throw new IllegalArgumentException("serialized timestamp must contain exactly 16 bytes");
    }

    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

    long physicalTime = buffer.getLong();
    long logicalCounter = buffer.getLong();

    return new HybridTimestamp(physicalTime, logicalCounter);
  }

  @Override
  public int compareTo(HybridTimestamp other) {
    Objects.requireNonNull(other, "Timestamp must not be null");

    int physicalComparison = Long.compare(physicalTime, other.physicalTime);

    if (physicalComparison != 0) {
      return physicalComparison;
    }

    return Long.compare(logicalCounter, other.logicalCounter);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof HybridTimestamp)) {
      return false;
    }

    HybridTimestamp other = (HybridTimestamp) obj;

    return physicalTime == other.physicalTime && logicalCounter == other.logicalCounter;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(physicalTime);
    result = 31 * result + Long.hashCode(logicalCounter);
    return result;
  }

  @Override
  public String toString() {
    return physicalTime + ":" + logicalCounter;
  }
}
