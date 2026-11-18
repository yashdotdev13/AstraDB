package io.astradb.transaction;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.common.time.TimestampOracle;
import io.astradb.storage.mvcc.Snapshot;
import io.astradb.storage.mvcc.SnapshotManager;
import io.astradb.storage.mvcc.TransactionId;
import io.astradb.storage.rocksdb.BatchOperation;
import io.astradb.storage.rocksdb.WalBackedRocksDbMvccStore;
import io.astradb.transaction.mutation.PendingMutation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.rocksdb.RocksDBException;

/**
 * Coordinates the lifecycle of local database transactions.
 *
 * <p>Active transactions are tracked in memory. Committed and aborted transactions are removed from
 * the active transaction registry.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class TransactionManager<K, V> {

  private final WalBackedRocksDbMvccStore<K, V> store;
  private final TimestampOracle timestampOracle;
  private final SnapshotManager snapshotManager;

  private final ConcurrentMap<TransactionId, Transaction<K, V>> activeTransactions =
      new ConcurrentHashMap<>();

  /**
   * Creates a transaction manager using the supplied storage and snapshot infrastructure.
   *
   * @param store WAL-backed MVCC storage
   * @param timestampOracle source of hybrid timestamps
   * @param snapshotManager manager for active transaction snapshots
   */
  public TransactionManager(
      WalBackedRocksDbMvccStore<K, V> store,
      TimestampOracle timestampOracle,
      SnapshotManager snapshotManager) {

    this.store = Objects.requireNonNull(store, "Storage must not be null");
    this.timestampOracle =
        Objects.requireNonNull(timestampOracle, "Timestamp oracle must not be null");
    this.snapshotManager =
        Objects.requireNonNull(snapshotManager, "Snapshot manager must not be null");
  }

  /**
   * Begins a transaction with a new identity and read snapshot.
   *
   * @return newly created transaction
   */
  public Transaction<K, V> begin() {
    TransactionId transactionId = TransactionId.generate();
    Snapshot snapshot = new Snapshot(transactionId, timestampOracle.now());

    snapshotManager.register(snapshot);

    Transaction<K, V> transaction = new Transaction<>(transactionId, snapshot, store);

    Transaction<K, V> existing = activeTransactions.putIfAbsent(transactionId, transaction);

    if (existing != null) {
      snapshotManager.release(transactionId);
      throw new TransactionException("Transaction already exists: " + transactionId);
    }

    return transaction;
  }

  /**
   * Returns an active transaction.
   *
   * @param transactionId transaction identity
   * @return active transaction
   * @throws TransactionException if the transaction is not active or is unknown
   */
  public Transaction<K, V> getTransaction(TransactionId transactionId) {
    Objects.requireNonNull(transactionId, "Transaction ID must not be null");

    Transaction<K, V> transaction = activeTransactions.get(transactionId);

    if (transaction == null) {
      throw new TransactionException("Active transaction not found: " + transactionId);
    }

    return transaction;
  }

  /**
   * Commits all staged mutations atomically.
   *
   * <p>If storage reports an error while applying the WAL-backed batch, the transaction is marked
   * IN_DOUBT because the durable WAL commit outcome cannot safely be inferred.
   *
   * @param transactionId transaction identity
   */
  public void commit(TransactionId transactionId) {
    Transaction<K, V> transaction = getTransaction(transactionId);

    synchronized (transaction) {
      transaction.beginCommit();

      List<PendingMutation<K, V>> pendingMutations = transaction.getPendingMutations();

      if (pendingMutations.isEmpty()) {
        transaction.markCommitted();
        complete(transactionId);
        return;
      }

      List<BatchOperation<K, V>> operations = new ArrayList<>(pendingMutations.size());

      for (PendingMutation<K, V> mutation : pendingMutations) {
        if (mutation.isDelete()) {
          operations.add(BatchOperation.delete(mutation.getKey()));
        } else {
          operations.add(BatchOperation.put(mutation.getKey(), mutation.getValue()));
        }
      }

      HybridTimestamp commitTimestamp;

      try {
        commitTimestamp = timestampOracle.now();
      } catch (RuntimeException exception) {
        transaction.markAborted();
        complete(transactionId);

        throw new TransactionException(
            "Failed to obtain commit timestamp for transaction: " + transactionId, exception);
      }

      try {
        store.applyBatch(operations, commitTimestamp);
      } catch (RocksDBException | IOException | RuntimeException exception) {
        transaction.markInDoubt();

        throw new TransactionException(
            "Transaction commit outcome is uncertain: " + transactionId, exception);
      }

      transaction.markCommitted();
      complete(transactionId);
    }
  }

  /**
   * Rolls back an active transaction and discards its staged mutations.
   *
   * @param transactionId transaction identity
   */
  public void rollback(TransactionId transactionId) {
    Transaction<K, V> transaction = getTransaction(transactionId);

    synchronized (transaction) {
      transaction.markAborted();
      complete(transactionId);
    }
  }

  private void complete(TransactionId transactionId) {
    activeTransactions.remove(transactionId);
    snapshotManager.release(transactionId);
  }
}
