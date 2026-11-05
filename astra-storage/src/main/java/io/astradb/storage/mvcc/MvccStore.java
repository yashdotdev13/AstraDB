package io.astradb.storage.mvcc;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.wal.WalRecord;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory MVCC storage engine that maintains version chains for keys.
 *
 * <p>Each key is associated with a version chain. Reads are evaluated against a snapshot timestamp
 * to provide consistent visibility.
 */
public final class MvccStore<K, V> {

  private final Map<K, VersionChain<V>> versionChains = new ConcurrentHashMap<>();

  /**
   * Adds a new version for the specified key.
   *
   * @param key key to store
   * @param value value to store
   * @param timestamp version timestamp
   */
  public void put(K key, V value, HybridTimestamp timestamp) {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    VersionChain<V> chain = versionChains.computeIfAbsent(key, ignored -> new VersionChain<>());

    chain.add(VersionedValue.of(timestamp, value));
  }

  /**
   * Marks the specified key as deleted at the given timestamp.
   *
   * @param key key to delete
   * @param timestamp deletion timestamp
   */
  public void delete(K key, HybridTimestamp timestamp) {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    VersionChain<V> chain = versionChains.computeIfAbsent(key, ignored -> new VersionChain<>());

    chain.add(VersionedValue.tombstone(timestamp));
  }

  /**
   * Reads the value visible to the specified snapshot.
   *
   * @param key key to read
   * @param snapshotTimestamp snapshot timestamp
   * @return visible value, or empty if absent or deleted
   */
  public Optional<V> get(K key, HybridTimestamp snapshotTimestamp) {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(snapshotTimestamp, "Snapshot timestamp must not be null");

    VersionChain<V> chain = versionChains.get(key);

    if (chain == null) {
      return Optional.empty();
    }

    return chain.get(snapshotTimestamp);
  }

  /**
   * Replays recovered WAL records into the MVCC store.
   *
   * <p>Only PUT and DELETE records are applied. Transaction markers are ignored.
   *
   * @param records committed records returned by WalRecovery
   */
  public void recover(List<WalRecord> records) {
    Objects.requireNonNull(records, "Recovered records must not be null");

    for (WalRecord record : records) {
      Objects.requireNonNull(record, "WAL record must not be null");

      switch (record.getType()) {
        case PUT:
          put(
              (K) new String(record.getKey()),
              (V) new String(record.getValue()),
              record.getTimestamp());
          break;

        case DELETE:
          delete((K) new String(record.getKey()), record.getTimestamp());
          break;

        case BEGIN:
        case COMMIT:
        case ABORT:
          // Transaction markers are not applied to MVCC.
          break;

        default:
          throw new IllegalArgumentException("Unsupported WAL record type: " + record.getType());
      }
    }
  }

  /**
   * Returns the number of keys tracked by this store.
   *
   * @return number of key-version chains
   */
  public int size() {
    return versionChains.size();
  }

  /**
   * Returns whether the store contains no keys.
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return versionChains.isEmpty();
  }

  /** Clears all stored versions. */
  public void clear() {
    versionChains.clear();
  }
}
