package io.astradb.storage.rocksdb;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.VersionChain;
import io.astradb.storage.mvcc.VersionedValue;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

public final class RocksDbMvccStore<K, V> implements AutoCloseable {

  private static final int LOCK_STRIPES = 256;

  private final RocksDbStore rocksDbStore;
  private final KeySerializer<K> keySerializer;
  private final VersionChainSerializer<V> versionChainSerializer;
  private final ReentrantLock[] locks = createLocks();

  private RocksDbMvccStore(
      RocksDbStore rocksDbStore,
      KeySerializer<K> keySerializer,
      VersionChainSerializer<V> versionChainSerializer) {

    this.rocksDbStore = Objects.requireNonNull(rocksDbStore);
    this.keySerializer = Objects.requireNonNull(keySerializer);
    this.versionChainSerializer = Objects.requireNonNull(versionChainSerializer);
  }

  public static <K, V> RocksDbMvccStore<K, V> open(
      Path databasePath, KeySerializer<K> keySerializer, ValueSerializer<V> valueSerializer)
      throws RocksDBException {

    RocksDbStore rocksDbStore = RocksDbStore.open(databasePath);

    return new RocksDbMvccStore<>(
        rocksDbStore, keySerializer, new VersionChainSerializer<>(valueSerializer));
  }

  public void put(K key, V value, HybridTimestamp timestamp) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    ReentrantLock lock = getKeyLock(key);
    lock.lock();

    try {
      VersionChain<V> chain = readVersionChain(key);
      chain.add(VersionedValue.of(timestamp, value));

      byte[] serializedKey = keySerializer.serialize(key);
      byte[] serializedChain = versionChainSerializer.serialize(chain);

      rocksDbStore.put(serializedKey, serializedChain);
    } finally {
      lock.unlock();
    }
  }

  public void delete(K key, HybridTimestamp timestamp) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    ReentrantLock lock = getKeyLock(key);
    lock.lock();

    try {
      VersionChain<V> chain = readVersionChain(key);
      chain.add(VersionedValue.tombstone(timestamp));

      byte[] serializedKey = keySerializer.serialize(key);
      byte[] serializedChain = versionChainSerializer.serialize(chain);

      rocksDbStore.put(serializedKey, serializedChain);
    } finally {
      lock.unlock();
    }
  }

  public V get(K key, HybridTimestamp timestamp) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    ReentrantLock lock = getKeyLock(key);
    lock.lock();

    try {
      VersionChain<V> chain = readVersionChain(key);
      return chain.get(timestamp).orElse(null);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Applies multiple mutations atomically using RocksDB WriteBatch.
   *
   * <p>Duplicate keys within a batch are rejected because a single transaction timestamp cannot
   * represent multiple versions of the same key.
   */
  void applyBatch(List<BatchOperation<K, V>> operations, HybridTimestamp timestamp)
      throws RocksDBException {

    Objects.requireNonNull(operations, "Operations must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    if (operations.isEmpty()) {
      throw new IllegalArgumentException("Batch must contain at least one operation");
    }

    Set<K> uniqueKeys = new HashSet<>();
    List<K> keys = new ArrayList<>(operations.size());

    for (BatchOperation<K, V> operation : operations) {
      Objects.requireNonNull(operation, "Batch operation must not be null");

      K key = operation.getKey();

      if (!uniqueKeys.add(key)) {
        throw new IllegalArgumentException("Duplicate key in batch: " + key);
      }

      keys.add(key);
    }

    List<ReentrantLock> acquiredLocks = acquireLocks(keys);

    try (WriteBatch writeBatch = new WriteBatch()) {
      for (BatchOperation<K, V> operation : operations) {
        K key = operation.getKey();

        VersionChain<V> chain = readVersionChain(key);

        if (operation.getType() == BatchOperation.Type.PUT) {
          chain.add(VersionedValue.of(timestamp, operation.getValue()));
        } else {
          chain.add(VersionedValue.tombstone(timestamp));
        }

        byte[] serializedKey = keySerializer.serialize(key);
        byte[] serializedChain = versionChainSerializer.serialize(chain);

        writeBatch.put(serializedKey, serializedChain);
      }

      rocksDbStore.write(writeBatch);
    } finally {
      releaseLocks(acquiredLocks);
    }
  }

  private VersionChain<V> readVersionChain(K key) throws RocksDBException {
    byte[] serializedKey = keySerializer.serialize(key);
    return readVersionChain(serializedKey);
  }

  private VersionChain<V> readVersionChain(byte[] serializedKey) throws RocksDBException {
    byte[] serializedChain = rocksDbStore.get(serializedKey);

    if (serializedChain == null) {
      return new VersionChain<>();
    }

    return versionChainSerializer.deserialize(serializedChain);
  }

  VersionChain<V> readVersionChainForKey(K key) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    return readVersionChain(key);
  }

  private List<ReentrantLock> acquireLocks(List<K> keys) {
    Set<Integer> lockIndexes = new HashSet<>();

    for (K key : keys) {
      lockIndexes.add(getLockIndex(key));
    }

    List<Integer> sortedIndexes = new ArrayList<>(lockIndexes);
    sortedIndexes.sort(Integer::compareTo);

    List<ReentrantLock> acquiredLocks = new ArrayList<>(sortedIndexes.size());

    for (Integer index : sortedIndexes) {
      ReentrantLock lock = locks[index];
      lock.lock();
      acquiredLocks.add(lock);
    }

    return acquiredLocks;
  }

  private void releaseLocks(List<ReentrantLock> acquiredLocks) {
    for (int i = acquiredLocks.size() - 1; i >= 0; i--) {
      acquiredLocks.get(i).unlock();
    }
  }

  private ReentrantLock getKeyLock(K key) {
    return locks[getLockIndex(key)];
  }

  private int getLockIndex(K key) {
    return Math.floorMod(key.hashCode(), LOCK_STRIPES);
  }

  private static ReentrantLock[] createLocks() {
    ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];

    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new ReentrantLock();
    }

    return locks;
  }

  public void flush() throws RocksDBException {
    rocksDbStore.flush();
  }

  @Override
  public void close() {
    rocksDbStore.close();
  }
}
