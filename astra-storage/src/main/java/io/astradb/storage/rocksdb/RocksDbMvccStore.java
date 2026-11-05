package io.astradb.storage.rocksdb;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.VersionChain;
import io.astradb.storage.mvcc.VersionedValue;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.rocksdb.RocksDBException;

/**
 * Persistent MVCC storage engine backed by RocksDB.
 *
 * <p>Each key maps to a serialized version chain containing its complete MVCC history.
 *
 * @param <K> key type
 * @param <V> value type
 */
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

    this.rocksDbStore = rocksDbStore;
    this.keySerializer = keySerializer;
    this.versionChainSerializer = versionChainSerializer;
  }

  /**
   * Opens or creates a persistent MVCC store.
   *
   * @param path database directory
   * @param keySerializer key serializer
   * @param valueSerializer value serializer
   * @param <K> key type
   * @param <V> value type
   * @return opened persistent MVCC store
   * @throws RocksDBException if the database cannot be opened
   */
  public static <K, V> RocksDbMvccStore<K, V> open(
      Path path, KeySerializer<K> keySerializer, ValueSerializer<V> valueSerializer)
      throws RocksDBException {

    Objects.requireNonNull(path, "Database path must not be null");
    Objects.requireNonNull(keySerializer, "Key serializer must not be null");
    Objects.requireNonNull(valueSerializer, "Value serializer must not be null");

    RocksDbStore rocksDbStore = RocksDbStore.open(path);

    return new RocksDbMvccStore<>(
        rocksDbStore, keySerializer, new VersionChainSerializer<>(valueSerializer));
  }

  /**
   * Adds a new version for the specified key.
   *
   * @param key key to store
   * @param value value to store
   * @param timestamp version timestamp
   * @throws RocksDBException if persistence fails
   */
  public void put(K key, V value, HybridTimestamp timestamp) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    ReentrantLock lock = getKeyLock(key);
    lock.lock();

    try {
      byte[] serializedKey = keySerializer.serialize(key);
      VersionChain<V> chain = readVersionChain(serializedKey);

      chain.add(VersionedValue.of(timestamp, value));

      rocksDbStore.put(serializedKey, versionChainSerializer.serialize(chain));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Marks the specified key as deleted at the given timestamp.
   *
   * @param key key to delete
   * @param timestamp deletion timestamp
   * @throws RocksDBException if persistence fails
   */
  public void delete(K key, HybridTimestamp timestamp) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    ReentrantLock lock = getKeyLock(key);
    lock.lock();

    try {
      byte[] serializedKey = keySerializer.serialize(key);
      VersionChain<V> chain = readVersionChain(serializedKey);

      chain.add(VersionedValue.tombstone(timestamp));

      rocksDbStore.put(serializedKey, versionChainSerializer.serialize(chain));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Reads the value visible to the specified snapshot.
   *
   * @param key key to read
   * @param snapshotTimestamp snapshot timestamp
   * @return visible value, or empty if absent or deleted
   * @throws RocksDBException if the read fails
   */
  public Optional<V> get(K key, HybridTimestamp snapshotTimestamp) throws RocksDBException {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(snapshotTimestamp, "Snapshot timestamp must not be null");

    byte[] serializedKey = keySerializer.serialize(key);
    byte[] serializedChain = rocksDbStore.get(serializedKey);

    if (serializedChain == null) {
      return Optional.empty();
    }

    VersionChain<V> chain = versionChainSerializer.deserialize(serializedChain);

    return chain.get(snapshotTimestamp);
  }

  private VersionChain<V> readVersionChain(byte[] serializedKey) throws RocksDBException {
    byte[] serializedChain = rocksDbStore.get(serializedKey);

    if (serializedChain == null) {
      return new VersionChain<>();
    }

    return versionChainSerializer.deserialize(serializedChain);
  }

  /** Flushes pending writes to persistent storage. */
  public void flush() throws RocksDBException {
    rocksDbStore.flush();
  }

  private ReentrantLock getKeyLock(K key) {
    int index = Math.floorMod(key.hashCode(), LOCK_STRIPES);
    return locks[index];
  }

  private static ReentrantLock[] createLocks() {
    ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];

    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new ReentrantLock();
    }

    return locks;
  }

  /** Closes the underlying RocksDB database. */
  @Override
  public void close() {
    rocksDbStore.close();
  }
}
