package io.astradb.storage.mvcc;

import io.astradb.common.time.HybridTimestamp;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages active transaction snapshots in the MVCC storage engine.
 *
 * <p>Tracks active snapshots to support consistent reads and future MVCC garbage collection.
 */
public final class SnapshotManager {

  private final Map<TransactionId, Snapshot> activeSnapshots = new ConcurrentHashMap<>();

  /**
   * Registers a snapshot for an active transaction.
   *
   * @param snapshot snapshot to register
   * @throws NullPointerException if snapshot is null
   * @throws IllegalArgumentException if the transaction is already registered
   */
  public void register(Snapshot snapshot) {
    Objects.requireNonNull(snapshot, "Snapshot must not be null");

    Snapshot existing = activeSnapshots.putIfAbsent(snapshot.getTransactionId(), snapshot);

    if (existing != null) {
      throw new IllegalArgumentException(
          "Snapshot already registered for transaction: " + snapshot.getTransactionId());
    }
  }

  /**
   * Returns the active snapshot for a transaction.
   *
   * @param transactionId transaction identifier
   * @return active snapshot, or empty if not registered
   */
  public Optional<Snapshot> get(TransactionId transactionId) {
    Objects.requireNonNull(transactionId, "Transaction ID must not be null");
    return Optional.ofNullable(activeSnapshots.get(transactionId));
  }

  /**
   * Releases the snapshot associated with a transaction.
   *
   * @param transactionId transaction identifier
   * @return true if a snapshot was removed, false if none existed
   */
  public boolean release(TransactionId transactionId) {
    Objects.requireNonNull(transactionId, "Transaction ID must not be null");
    return activeSnapshots.remove(transactionId) != null;
  }

  /**
   * Returns the oldest timestamp among currently active snapshots.
   *
   * @return oldest active timestamp, or empty if no snapshots are active
   */
  public Optional<HybridTimestamp> getOldestActiveTimestamp() {
    return activeSnapshots.values().stream()
        .map(Snapshot::getTimestamp)
        .min(HybridTimestamp::compareTo);
  }

  /**
   * Returns the number of currently active snapshots.
   *
   * @return active snapshot count
   */
  public int size() {
    return activeSnapshots.size();
  }

  /**
   * Returns whether there are no active snapshots.
   *
   * @return true if no snapshots are active
   */
  public boolean isEmpty() {
    return activeSnapshots.isEmpty();
  }
}
