package io.astradb.storage.mvcc;

import io.astradb.common.time.HybridTimestamp;
import java.util.Objects;

/**
 * Represents a consistent point-in-time view of the database for a transaction.
 *
 * <p>The snapshot timestamp determines which versions are visible to the transaction.
 */
public final class Snapshot {

  private final TransactionId transactionId;
  private final HybridTimestamp timestamp;

  /**
   * Creates a snapshot for a transaction.
   *
   * @param transactionId unique transaction identifier
   * @param timestamp snapshot read timestamp
   */
  public Snapshot(TransactionId transactionId, HybridTimestamp timestamp) {
    this.transactionId = Objects.requireNonNull(transactionId, "Transaction ID must not be null");
    this.timestamp = Objects.requireNonNull(timestamp, "Snapshot timestamp must not be null");
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public HybridTimestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof Snapshot)) {
      return false;
    }
    Snapshot that = (Snapshot) object;
    return transactionId.equals(that.transactionId) && timestamp.equals(that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionId, timestamp);
  }

  @Override
  public String toString() {
    return "Snapshot{transactionId=" + transactionId + ", timestamp=" + timestamp + '}';
  }
}
