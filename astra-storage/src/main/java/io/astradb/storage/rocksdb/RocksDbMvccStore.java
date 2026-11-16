package io.astradb.storage.rocksdb;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.VersionChain;
import io.astradb.storage.mvcc.VersionedValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Persistent MVCC storage backed by RocksDB.
 *
 * <p>Maintains version chains for keys and supports consistent database snapshot export and
 * restoration.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class RocksDbMvccStore<K, V> implements AutoCloseable {

  private static final int LOCK_STRIPES = 256;

  private static final int SNAPSHOT_MAGIC = 0x41535452;
  private static final int SNAPSHOT_FORMAT_VERSION = 1;

  private final RocksDbStore rocksDbStore;
  private final KeySerializer<K> keySerializer;
  private final VersionChainSerializer<V> versionChainSerializer;

  private final ReentrantLock[] locks = createLocks();

  /**
   * Coordinates normal database operations with snapshot export and restoration.
   *
   * <p>Normal operations acquire the read lock. Snapshot export and restoration acquire the write
   * lock, ensuring that no database mutation or read overlaps a snapshot operation.
   */
  private final ReentrantReadWriteLock snapshotLock = new ReentrantReadWriteLock();

  private RocksDbMvccStore(
      RocksDbStore rocksDbStore,
      KeySerializer<K> keySerializer,
      VersionChainSerializer<V> versionChainSerializer) {

    this.rocksDbStore = Objects.requireNonNull(rocksDbStore);
    this.keySerializer = Objects.requireNonNull(keySerializer);
    this.versionChainSerializer = Objects.requireNonNull(versionChainSerializer);
  }

  /**
   * Opens or creates a RocksDB-backed MVCC store.
   *
   * @param databasePath database directory
   * @param keySerializer key serializer
   * @param valueSerializer value serializer
   * @param <K> key type
   * @param <V> value type
   * @return opened MVCC store
   * @throws RocksDBException if the database cannot be opened
   */
  public static <K, V> RocksDbMvccStore<K, V> open(
      Path databasePath, KeySerializer<K> keySerializer, ValueSerializer<V> valueSerializer)
      throws RocksDBException {

    Objects.requireNonNull(databasePath, "Database path must not be null");
    Objects.requireNonNull(keySerializer, "Key serializer must not be null");
    Objects.requireNonNull(valueSerializer, "Value serializer must not be null");

    RocksDbStore rocksDbStore = RocksDbStore.open(databasePath);

    return new RocksDbMvccStore<>(
        rocksDbStore, keySerializer, new VersionChainSerializer<>(valueSerializer));
  }

  /**
   * Adds a new version for the specified key.
   *
   * @param key key
   * @param value value
   * @param timestamp version timestamp
   * @throws RocksDBException if the write fails
   */
  public void put(K key, V value, HybridTimestamp timestamp) throws RocksDBException {
    snapshotLock.readLock().lock();

    try {
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
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  /**
   * Marks the specified key as deleted.
   *
   * @param key key
   * @param timestamp deletion timestamp
   * @throws RocksDBException if the write fails
   */
  public void delete(K key, HybridTimestamp timestamp) throws RocksDBException {
    snapshotLock.readLock().lock();

    try {
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
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  /**
   * Reads the value visible at the specified snapshot timestamp.
   *
   * @param key key
   * @param timestamp snapshot timestamp
   * @return visible value, or null if absent or deleted
   * @throws RocksDBException if the read fails
   */
  public V get(K key, HybridTimestamp timestamp) throws RocksDBException {
    snapshotLock.readLock().lock();

    try {
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
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  /**
   * Applies multiple mutations atomically using RocksDB WriteBatch.
   *
   * <p>Duplicate keys within a batch are rejected because a single transaction timestamp cannot
   * represent multiple versions of the same key.
   *
   * @param operations operations to apply
   * @param timestamp transaction timestamp
   * @throws RocksDBException if the batch write fails
   */
  void applyBatch(List<BatchOperation<K, V>> operations, HybridTimestamp timestamp)
      throws RocksDBException {

    snapshotLock.readLock().lock();

    try {
      Objects.requireNonNull(operations, "Operations must not be null");
      Objects.requireNonNull(timestamp, "Timestamp must not be null");

      if (operations.isEmpty()) {
        throw new IllegalArgumentException("Batch must contain at least one operation");
      }

      Set<K> uniqueKeys = new HashSet<>();
      List<K> keys = new ArrayList<>(operations.size());

      for (BatchOperation<K, V> operation : operations) {
        Objects.requireNonNull(operation, "Batch operation must not be null");

        K key = Objects.requireNonNull(operation.getKey(), "Batch key must not be null");

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
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  /**
   * Exports the complete MVCC database state into a portable snapshot.
   *
   * <p>All versions and tombstones are preserved. The snapshot is captured while holding the
   * snapshot write lock, preventing concurrent database modifications.
   *
   * @return serialized database snapshot
   * @throws RocksDBException if reading RocksDB fails
   */
  public byte[] exportSnapshot() throws RocksDBException {
    snapshotLock.writeLock().lock();

    try {
      List<RocksDbEntry> entries = rocksDbStore.readAll();

      try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          DataOutputStream output = new DataOutputStream(byteStream)) {

        output.writeInt(SNAPSHOT_MAGIC);
        output.writeInt(SNAPSHOT_FORMAT_VERSION);
        output.writeInt(entries.size());

        for (RocksDbEntry entry : entries) {
          byte[] key = entry.getKey();
          byte[] value = entry.getValue();

          output.writeInt(key.length);
          output.write(key);

          output.writeInt(value.length);
          output.write(value);
        }

        output.flush();
        return byteStream.toByteArray();
      } catch (IOException exception) {
        throw new IllegalStateException("Failed to serialize MVCC snapshot", exception);
      }
    } finally {
      snapshotLock.writeLock().unlock();
    }
  }

  /**
   * Restores the complete MVCC database state from a serialized snapshot.
   *
   * <p>The snapshot is validated before the existing database is replaced.
   *
   * @param snapshot serialized database snapshot
   * @throws RocksDBException if replacing RocksDB contents fails
   * @throws IllegalArgumentException if the snapshot is invalid
   */
  public void restoreSnapshot(byte[] snapshot) throws RocksDBException {
    Objects.requireNonNull(snapshot, "Snapshot must not be null");

    snapshotLock.writeLock().lock();

    try {
      List<RocksDbEntry> entries = new ArrayList<>();
      Set<String> serializedKeys = new HashSet<>();

      try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(snapshot))) {

        int magic = input.readInt();

        if (magic != SNAPSHOT_MAGIC) {
          throw new IllegalArgumentException("Invalid MVCC snapshot magic");
        }

        int version = input.readInt();

        if (version != SNAPSHOT_FORMAT_VERSION) {
          throw new IllegalArgumentException(
              "Unsupported MVCC snapshot format version: " + version);
        }

        int entryCount = input.readInt();

        if (entryCount < 0 || entryCount > input.available() / (2 * Integer.BYTES)) {
          throw new IllegalArgumentException("Invalid MVCC snapshot entry count");
        }

        for (int i = 0; i < entryCount; i++) {
          int keyLength = input.readInt();

          if (keyLength < 0 || keyLength > input.available() - Integer.BYTES) {
            throw new IllegalArgumentException("Invalid snapshot key length");
          }

          byte[] key = input.readNBytes(keyLength);

          if (key.length != keyLength) {
            throw new IllegalArgumentException("Incomplete snapshot key");
          }

          int valueLength = input.readInt();

          if (valueLength < 0 || valueLength > input.available()) {
            throw new IllegalArgumentException("Invalid snapshot value length");
          }

          byte[] value = input.readNBytes(valueLength);

          if (value.length != valueLength) {
            throw new IllegalArgumentException("Incomplete snapshot value");
          }

          String encodedKey = Base64.getEncoder().encodeToString(key);

          if (!serializedKeys.add(encodedKey)) {
            throw new IllegalArgumentException("Duplicate key in MVCC snapshot");
          }

          K deserializedKey = keySerializer.deserialize(key);

          if (deserializedKey == null) {
            throw new IllegalArgumentException("Snapshot contains a null key");
          }

          VersionChain<V> chain = versionChainSerializer.deserialize(value);

          if (chain.isEmpty()) {
            throw new IllegalArgumentException("Snapshot contains an empty version chain");
          }

          entries.add(new RocksDbEntry(key, value));
        }

        if (input.available() != 0) {
          throw new IllegalArgumentException("Unexpected trailing bytes in MVCC snapshot");
        }
      } catch (IOException exception) {
        throw new IllegalArgumentException("Failed to deserialize MVCC snapshot", exception);
      }

      rocksDbStore.replaceAll(entries);
    } finally {
      snapshotLock.writeLock().unlock();
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

    snapshotLock.readLock().lock();

    try {
      return readVersionChain(key);
    } finally {
      snapshotLock.readLock().unlock();
    }
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

  /**
   * Flushes pending writes to persistent storage.
   *
   * @throws RocksDBException if the flush fails
   */
  public void flush() throws RocksDBException {
    snapshotLock.readLock().lock();

    try {
      rocksDbStore.flush();
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    snapshotLock.writeLock().lock();

    try {
      rocksDbStore.close();
    } finally {
      snapshotLock.writeLock().unlock();
    }
  }
}
