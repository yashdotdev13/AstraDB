package io.astradb.transaction;

import io.astradb.storage.mvcc.Snapshot;
import io.astradb.storage.mvcc.TransactionId;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import io.astradb.transaction.mutation.PendingMutation;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.rocksdb.RocksDBException;

/**
 * Represents a transaction with snapshot-based reads and staged mutations.
 *
 * <p>Mutations remain in memory until the transaction manager commits them.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class Transaction<K, V> {

  private final TransactionId id;
  private final Snapshot snapshot;
  private final WalBackedRocksDbMvccStore<K, V> store;

  private final Map<K, PendingMutation<K, V>> pendingMutations = new LinkedHashMap<>();

  private TransactionState state = TransactionState.ACTIVE;

  Transaction(TransactionId id, Snapshot snapshot, WalBackedRocksDbMvccStore<K, V> store) {

    this.id = Objects.requireNonNull(id, "Transaction ID must not be null");
    this.snapshot = Objects.requireNonNull(snapshot, "Snapshot must not be null");
    this.store = Objects.requireNonNull(store, "Storage must not be null");

    if (!id.equals(snapshot.getTransactionId())) {
      throw new IllegalArgumentException("Transaction ID must match the snapshot transaction ID");
    }
  }

  public TransactionId getId() {
    return id;
  }

  public io.astradb.common.time.HybridTimestamp getReadTimestamp() {
    return snapshot.getTimestamp();
  }

  public synchronized TransactionState getState() {
    return state;
  }

  /**
   * Stages a value mutation.
   *
   * <p>If the key already has a pending mutation, the new mutation replaces it.
   */
  public synchronized void put(K key, V value) {
    ensureActive();

    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");

    pendingMutations.put(key, PendingMutation.put(key, value));
  }

  /** Stages a deletion mutation. */
  public synchronized void delete(K key) {
    ensureActive();

    Objects.requireNonNull(key, "Key must not be null");

    pendingMutations.put(key, PendingMutation.delete(key));
  }

  /**
   * Reads a value using read-your-writes semantics and the transaction snapshot.
   *
   * @param key key to read
   * @return pending or snapshot-visible value, or empty if absent or deleted
   */
  public synchronized Optional<V> get(K key) {
    ensureActive();

    Objects.requireNonNull(key, "Key must not be null");

    PendingMutation<K, V> pendingMutation = pendingMutations.get(key);

    if (pendingMutation != null) {
      return pendingMutation.isDelete()
          ? Optional.empty()
          : Optional.of(pendingMutation.getValue());
    }

    try {
      return store.get(key, snapshot.getTimestamp());
    } catch (RocksDBException exception) {
      throw new TransactionException("Failed to read key within transaction: " + id, exception);
    }
  }

  /** Returns an immutable snapshot of the staged mutations. */
  synchronized List<PendingMutation<K, V>> getPendingMutations() {
    return List.copyOf(pendingMutations.values());
  }

  /** Transitions an active transaction into the committing state. */
  synchronized void beginCommit() {
    ensureActive();
    state = TransactionState.COMMITTING;
  }

  /** Marks a transaction as successfully committed. */
  synchronized void markCommitted() {
    requireState(TransactionState.COMMITTING);
    state = TransactionState.COMMITTED;
  }

  /** Marks an active or committing transaction as aborted. */
  synchronized void markAborted() {
    if (state != TransactionState.ACTIVE && state != TransactionState.COMMITTING) {
      throw new TransactionException("Cannot abort transaction " + id + " in state " + state);
    }

    pendingMutations.clear();
    state = TransactionState.ABORTED;
  }

  /** Marks a committing transaction whose outcome cannot be determined. */
  synchronized void markInDoubt() {
    requireState(TransactionState.COMMITTING);
    state = TransactionState.IN_DOUBT;
  }

  private void ensureActive() {
    requireState(TransactionState.ACTIVE);
  }

  private void requireState(TransactionState expected) {
    if (state != expected) {
      throw new TransactionException(
          "Transaction " + id + " must be in state " + expected + " but is " + state);
    }
  }
}
