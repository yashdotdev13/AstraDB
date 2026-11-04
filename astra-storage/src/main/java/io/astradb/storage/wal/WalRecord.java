package io.astradb.storage.wal;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable representation of a write-ahead log record.
 *
 * <p>A record captures a transaction operation and its position in the WAL. Byte-array payloads are
 * defensively copied to preserve immutability.
 */
public final class WalRecord {

  private final LogSequenceNumber lsn;
  private final TransactionId transactionId;
  private final WalRecordType type;
  private final HybridTimestamp timestamp;
  private final byte[] key;
  private final byte[] value;

  WalRecord(
      LogSequenceNumber lsn,
      TransactionId transactionId,
      WalRecordType type,
      HybridTimestamp timestamp,
      byte[] key,
      byte[] value) {

    this.lsn = Objects.requireNonNull(lsn, "LSN must not be null");
    this.transactionId = Objects.requireNonNull(transactionId, "Transaction ID must not be null");
    this.type = Objects.requireNonNull(type, "Record type must not be null");
    this.timestamp = Objects.requireNonNull(timestamp, "Timestamp must not be null");

    validatePayload(type, key, value);

    this.key = key == null ? null : Arrays.copyOf(key, key.length);
    this.value = value == null ? null : Arrays.copyOf(value, value.length);
  }

  public static WalRecord begin(
      LogSequenceNumber lsn, TransactionId transactionId, HybridTimestamp timestamp) {
    return new WalRecord(lsn, transactionId, WalRecordType.BEGIN, timestamp, null, null);
  }

  public static WalRecord put(
      LogSequenceNumber lsn,
      TransactionId transactionId,
      HybridTimestamp timestamp,
      byte[] key,
      byte[] value) {
    return new WalRecord(lsn, transactionId, WalRecordType.PUT, timestamp, key, value);
  }

  public static WalRecord delete(
      LogSequenceNumber lsn, TransactionId transactionId, HybridTimestamp timestamp, byte[] key) {
    return new WalRecord(lsn, transactionId, WalRecordType.DELETE, timestamp, key, null);
  }

  public static WalRecord commit(
      LogSequenceNumber lsn, TransactionId transactionId, HybridTimestamp timestamp) {
    return new WalRecord(lsn, transactionId, WalRecordType.COMMIT, timestamp, null, null);
  }

  public static WalRecord abort(
      LogSequenceNumber lsn, TransactionId transactionId, HybridTimestamp timestamp) {
    return new WalRecord(lsn, transactionId, WalRecordType.ABORT, timestamp, null, null);
  }

  private static void validatePayload(WalRecordType type, byte[] key, byte[] value) {
    switch (type) {
      case PUT:
        requireNonEmpty(key, "PUT key");
        requireNonEmpty(value, "PUT value");
        break;

      case DELETE:
        requireNonEmpty(key, "DELETE key");
        if (value != null) {
          throw new IllegalArgumentException("DELETE record must not contain a value");
        }
        break;

      case BEGIN:
      case COMMIT:
      case ABORT:
        if (key != null || value != null) {
          throw new IllegalArgumentException(type + " record must not contain a payload");
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported WAL record type: " + type);
    }
  }

  private static void requireNonEmpty(byte[] bytes, String fieldName) {
    if (bytes == null || bytes.length == 0) {
      throw new IllegalArgumentException(fieldName + " must not be null or empty");
    }
  }

  public LogSequenceNumber getLsn() {
    return lsn;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public WalRecordType getType() {
    return type;
  }

  public HybridTimestamp getTimestamp() {
    return timestamp;
  }

  public byte[] getKey() {
    return key == null ? null : Arrays.copyOf(key, key.length);
  }

  public byte[] getValue() {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof WalRecord)) {
      return false;
    }
    WalRecord that = (WalRecord) object;
    return lsn.equals(that.lsn)
        && transactionId.equals(that.transactionId)
        && type == that.type
        && timestamp.equals(that.timestamp)
        && Arrays.equals(key, that.key)
        && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(lsn, transactionId, type, timestamp);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "WalRecord{"
        + "lsn="
        + lsn
        + ", transactionId="
        + transactionId
        + ", type="
        + type
        + ", timestamp="
        + timestamp
        + '}';
  }
}
