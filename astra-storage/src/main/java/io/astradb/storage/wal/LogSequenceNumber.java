package io.astradb.storage.wal;

/**
 * Represents the position of a record in the write-ahead log.
 *
 * <p>Log sequence numbers are monotonically increasing and establish the ordering of WAL records.
 */
public final class LogSequenceNumber implements Comparable<LogSequenceNumber> {

  private final long value;

  private LogSequenceNumber(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Log sequence number cannot be negative");
    }
    this.value = value;
  }

  /**
   * Creates a log sequence number.
   *
   * @param value numeric sequence number
   * @return log sequence number
   */
  public static LogSequenceNumber of(long value) {
    return new LogSequenceNumber(value);
  }

  /**
   * Returns the numeric value of this sequence number.
   *
   * @return sequence number
   */
  public long getValue() {
    return value;
  }

  @Override
  public int compareTo(LogSequenceNumber other) {
    if (other == null) {
      throw new NullPointerException("Other log sequence number must not be null");
    }
    return Long.compare(this.value, other.value);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof LogSequenceNumber)) {
      return false;
    }
    LogSequenceNumber that = (LogSequenceNumber) object;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(value);
  }

  @Override
  public String toString() {
    return "LSN{" + value + '}';
  }
}
