package io.astradb.storage.rocksdb;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import io.astradb.storage.mvcc.VersionChain;
import io.astradb.storage.mvcc.VersionedValue;
import io.astradb.storage.wal.WalManager;
import io.astradb.storage.wal.WalRecord;
import io.astradb.storage.wal.WalRecordType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.rocksdb.RocksDBException;

/**
 * MVCC store backed by RocksDB and a write-ahead log.
 *
 * <p>Each transaction follows WAL-first durability:
 *
 * <ol>
 *   <li>Append the transaction and its operations to the WAL.
 *   <li>Commit and force the WAL.
 *   <li>Apply the transaction atomically to RocksDB.
 * </ol>
 *
 * <p>Committed WAL transactions are replayed during initialization.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class WalBackedRocksDbMvccStore<K, V> implements AutoCloseable {

  private final RocksDbMvccStore<K, V> store;
  private final WalManager walManager;
  private final KeySerializer<K> keySerializer;
  private final ValueSerializer<V> valueSerializer;

  private WalBackedRocksDbMvccStore(
      RocksDbMvccStore<K, V> store,
      WalManager walManager,
      KeySerializer<K> keySerializer,
      ValueSerializer<V> valueSerializer) {

    this.store = Objects.requireNonNull(store, "Store must not be null");
    this.walManager = Objects.requireNonNull(walManager, "WAL manager must not be null");
    this.keySerializer = Objects.requireNonNull(keySerializer, "Key serializer must not be null");
    this.valueSerializer =
        Objects.requireNonNull(valueSerializer, "Value serializer must not be null");
  }

  /**
   * Opens the WAL-backed MVCC store and replays committed WAL transactions.
   *
   * @param databasePath RocksDB directory
   * @param walPath WAL file path
   * @param keySerializer key serializer
   * @param valueSerializer value serializer
   * @return opened store
   * @throws RocksDBException if RocksDB cannot be opened
   * @throws IOException if WAL initialization or recovery fails
   */
  public static <K, V> WalBackedRocksDbMvccStore<K, V> open(
      Path databasePath,
      Path walPath,
      KeySerializer<K> keySerializer,
      ValueSerializer<V> valueSerializer)
      throws RocksDBException, IOException {

    Objects.requireNonNull(databasePath, "Database path must not be null");
    Objects.requireNonNull(walPath, "WAL path must not be null");
    Objects.requireNonNull(keySerializer, "Key serializer must not be null");
    Objects.requireNonNull(valueSerializer, "Value serializer must not be null");

    RocksDbMvccStore<K, V> store =
        RocksDbMvccStore.open(databasePath, keySerializer, valueSerializer);

    WalManager walManager = new WalManager(walPath);

    WalBackedRocksDbMvccStore<K, V> backedStore =
        new WalBackedRocksDbMvccStore<>(store, walManager, keySerializer, valueSerializer);

    try {
      backedStore.recover();
      return backedStore;
    } catch (RocksDBException | IOException | RuntimeException exception) {
      try {
        backedStore.close();
      } catch (Exception closeException) {
        exception.addSuppressed(closeException);
      }

      throw exception;
    }
  }

  /**
   * Stores a value using WAL-first durability.
   *
   * @param key key
   * @param value value
   * @param timestamp MVCC timestamp
   * @throws RocksDBException if RocksDB application fails
   * @throws IOException if WAL writing fails
   */
  public synchronized void put(K key, V value, HybridTimestamp timestamp)
      throws RocksDBException, IOException {

    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    BatchOperation<K, V> operation = BatchOperation.put(key, value);

    applyBatch(List.of(operation), timestamp);
  }

  /**
   * Deletes a value using WAL-first durability.
   *
   * @param key key
   * @param timestamp deletion timestamp
   * @throws RocksDBException if RocksDB application fails
   * @throws IOException if WAL writing fails
   */
  public synchronized void delete(K key, HybridTimestamp timestamp)
      throws RocksDBException, IOException {

    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    BatchOperation<K, V> operation = BatchOperation.delete(key);

    applyBatch(List.of(operation), timestamp);
  }

  /**
   * Atomically applies multiple mutations using one WAL transaction and one RocksDB WriteBatch.
   *
   * <p>Duplicate keys are rejected because a single transaction timestamp cannot represent multiple
   * versions of the same key.
   *
   * @param operations mutations to apply
   * @param timestamp transaction timestamp
   * @throws RocksDBException if RocksDB application fails
   * @throws IOException if WAL writing fails
   */
  public synchronized void applyBatch(
      List<BatchOperation<K, V>> operations, HybridTimestamp timestamp)
      throws RocksDBException, IOException {

    Objects.requireNonNull(operations, "Operations must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    if (operations.isEmpty()) {
      throw new IllegalArgumentException("Batch must contain at least one operation");
    }

    Set<K> uniqueKeys = new HashSet<>();

    // Validate and serialize all operations before writing anything to the WAL.
    List<SerializedBatchOperation<K, V>> serializedOperations = new ArrayList<>(operations.size());

    for (BatchOperation<K, V> operation : operations) {
      Objects.requireNonNull(operation, "Batch operation must not be null");

      K key = Objects.requireNonNull(operation.getKey(), "Batch key must not be null");

      if (!uniqueKeys.add(key)) {
        throw new IllegalArgumentException("Duplicate key in batch: " + key);
      }

      byte[] serializedKey = keySerializer.serialize(key);
      byte[] serializedValue = null;

      if (operation.getType() == BatchOperation.Type.PUT) {
        V value = Objects.requireNonNull(operation.getValue(), "Batch value must not be null");
        serializedValue = valueSerializer.serialize(value);
      }

      serializedOperations.add(
          new SerializedBatchOperation<>(operation.getType(), key, serializedKey, serializedValue));
    }

    TransactionId transactionId = TransactionId.generate();

    // WAL phase: persist the complete transaction before applying it to RocksDB.
    walManager.begin(transactionId, timestamp);

    for (SerializedBatchOperation<K, V> operation : serializedOperations) {
      if (operation.type == BatchOperation.Type.PUT) {
        walManager.put(
            transactionId, timestamp, operation.serializedKey, operation.serializedValue);
      } else {
        walManager.delete(transactionId, timestamp, operation.serializedKey);
      }
    }

    // Commit forces the WAL before the RocksDB transaction is applied.
    walManager.commit(transactionId, timestamp);

    // RocksDB phase: all mutations are applied atomically.
    store.applyBatch(operations, timestamp);
  }

  /**
   * Reads the value visible to the specified snapshot.
   *
   * @param key key
   * @param snapshotTimestamp snapshot timestamp
   * @return visible value, or empty if absent or deleted
   * @throws RocksDBException if the read fails
   */
  public Optional<V> get(K key, HybridTimestamp snapshotTimestamp) throws RocksDBException {

    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(snapshotTimestamp, "Snapshot timestamp must not be null");

    return Optional.ofNullable(store.get(key, snapshotTimestamp));
  }

  /**
   * Flushes RocksDB and the WAL.
   *
   * @throws RocksDBException if RocksDB flush fails
   * @throws IOException if WAL flush fails
   */
  public synchronized void flush() throws RocksDBException, IOException {
    store.flush();
    walManager.flush();
  }

  /**
   * Replays committed WAL transactions into RocksDB.
   *
   * <p>Uncommitted and aborted transactions are ignored. Each committed transaction is applied
   * atomically. Repeated replay is safe because identical versions are skipped.
   *
   * @throws IOException if WAL recovery fails
   * @throws RocksDBException if RocksDB application fails
   */
  private void recover() throws IOException, RocksDBException {

    List<WalRecord> records = walManager.readAll();

    Map<TransactionId, List<WalRecord>> transactions = new HashMap<>();
    Set<TransactionId> committedTransactions = new HashSet<>();
    Set<TransactionId> abortedTransactions = new HashSet<>();

    // Group records by transaction while identifying committed/aborted ones.
    for (WalRecord record : records) {
      TransactionId transactionId = record.getTransactionId();

      transactions.computeIfAbsent(transactionId, ignored -> new ArrayList<>()).add(record);

      if (record.getType() == WalRecordType.COMMIT) {
        committedTransactions.add(transactionId);
      } else if (record.getType() == WalRecordType.ABORT) {
        abortedTransactions.add(transactionId);
      }
    }

    // Replay only committed transactions that were not aborted.
    for (Map.Entry<TransactionId, List<WalRecord>> entry : transactions.entrySet()) {

      TransactionId transactionId = entry.getKey();

      if (!committedTransactions.contains(transactionId)
          || abortedTransactions.contains(transactionId)) {
        continue;
      }

      List<BatchOperation<K, V>> operations = new ArrayList<>();
      HybridTimestamp transactionTimestamp = null;

      for (WalRecord record : entry.getValue()) {

        WalRecordType type = record.getType();

        if (type != WalRecordType.PUT && type != WalRecordType.DELETE) {
          continue;
        }

        HybridTimestamp recordTimestamp = record.getTimestamp();

        if (transactionTimestamp == null) {
          transactionTimestamp = recordTimestamp;
        } else if (!transactionTimestamp.equals(recordTimestamp)) {
          throw new IOException("Inconsistent timestamps within WAL transaction: " + transactionId);
        }

        K key = keySerializer.deserialize(record.getKey());

        if (type == WalRecordType.PUT) {
          V value = valueSerializer.deserialize(record.getValue());
          operations.add(BatchOperation.put(key, value));
        } else {
          operations.add(BatchOperation.delete(key));
        }
      }

      if (operations.isEmpty()) {
        continue;
      }

      applyRecoveredBatch(operations, transactionTimestamp);
    }
  }

  /**
   * Applies a recovered transaction atomically, skipping versions that already exist with identical
   * contents.
   */
  private void applyRecoveredBatch(List<BatchOperation<K, V>> operations, HybridTimestamp timestamp)
      throws RocksDBException {

    List<BatchOperation<K, V>> missingOperations = new ArrayList<>();

    for (BatchOperation<K, V> operation : operations) {

      K key = operation.getKey();
      VersionChain<V> chain = store.readVersionChainForKey(key);

      boolean tombstone = operation.getType() == BatchOperation.Type.DELETE;
      V value = tombstone ? null : operation.getValue();

      if (containsIdenticalVersion(chain, timestamp, value, tombstone)) {
        continue;
      }

      missingOperations.add(operation);
    }

    if (!missingOperations.isEmpty()) {
      store.applyBatch(missingOperations, timestamp);
    }
  }

  /**
   * Checks whether a version with the specified timestamp already exists.
   *
   * @throws IllegalStateException if an existing version conflicts with WAL data
   */
  private boolean containsIdenticalVersion(
      VersionChain<V> chain, HybridTimestamp timestamp, V value, boolean tombstone) {

    for (VersionedValue<V> version : chain.getVersions()) {

      if (!version.getTimestamp().equals(timestamp)) {
        continue;
      }

      if (version.isTombstone() != tombstone) {
        throw new IllegalStateException("Conflicting recovered version at timestamp " + timestamp);
      }

      if (!tombstone && !Objects.equals(version.getValue(), value)) {
        throw new IllegalStateException("Conflicting recovered value at timestamp " + timestamp);
      }

      return true;
    }

    return false;
  }

  /** Internal representation of a pre-serialized batch mutation. */
  private static final class SerializedBatchOperation<K, V> {

    private final BatchOperation.Type type;
    private final K key;
    private final byte[] serializedKey;
    private final byte[] serializedValue;

    private SerializedBatchOperation(
        BatchOperation.Type type, K key, byte[] serializedKey, byte[] serializedValue) {

      this.type = Objects.requireNonNull(type);
      this.key = Objects.requireNonNull(key);
      this.serializedKey = Objects.requireNonNull(serializedKey);
      this.serializedValue = serializedValue;
    }
  }

  /**
   * Closes RocksDB and the WAL manager.
   *
   * @throws Exception if closing either resource fails
   */
  @Override
  public synchronized void close() throws Exception {

    Exception failure = null;

    try {
      store.close();
    } catch (Exception exception) {
      failure = exception;
    }

    try {
      walManager.close();
    } catch (Exception exception) {
      if (failure == null) {
        failure = exception;
      } else {
        failure.addSuppressed(exception);
      }
    }

    if (failure != null) {
      throw failure;
    }
  }
}
