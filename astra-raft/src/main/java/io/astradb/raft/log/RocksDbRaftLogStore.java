package io.astradb.raft.log;

import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.storage.rocksdb.BatchOperation;
import io.astradb.storage.rocksdb.RocksDbStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rocksdb.RocksDBException;

/** RocksDB-backed persistent storage for the Raft replicated log. */
public final class RocksDbRaftLogStore implements RaftLogStore, RaftMetadataStore {

  private static final byte[] ENTRY_PREFIX = "raft:entry:".getBytes(StandardCharsets.UTF_8);

  private static final byte[] LAST_INDEX_KEY =
      "raft:metadata:last-index".getBytes(StandardCharsets.UTF_8);

  private static final byte[] RAFT_METADATA_KEY =
      "raft:metadata:state".getBytes(StandardCharsets.UTF_8);

  private final RocksDbStore store;
  private final RaftLogSerializer serializer;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private long lastLogIndex;

  private RocksDbRaftLogStore(RocksDbStore store) {
    this.store = Objects.requireNonNull(store);
    this.serializer = new RaftLogSerializer();
    this.lastLogIndex = loadLastLogIndex();
    validateRecoveredLog();
  }

  /**
   * Opens or creates a persistent Raft log database.
   *
   * @param databasePath database directory
   * @return opened Raft log store
   */
  public static RocksDbRaftLogStore open(Path databasePath) {
    Objects.requireNonNull(databasePath, "Database path must not be null");

    RocksDbStore store = null;

    try {
      store = RocksDbStore.open(databasePath);
      return new RocksDbRaftLogStore(store);
    } catch (RocksDBException exception) {
      closeQuietly(store);
      throw new RaftLogException("Failed to open Raft log database", exception);
    } catch (RuntimeException exception) {
      closeQuietly(store);
      throw exception;
    }
  }

  private static void closeQuietly(RocksDbStore store) {
    if (store != null) {
      try {
        store.close();
      } catch (RuntimeException ignored) {
        // Preserve the original exception.
      }
    }
  }

  @Override
  public void append(RaftLogEntry entry) {
    appendAll(List.of(Objects.requireNonNull(entry)));
  }

  @Override
  public void appendAll(List<RaftLogEntry> entries) {
    Objects.requireNonNull(entries, "Entries must not be null");

    if (entries.isEmpty()) {
      return;
    }

    lock.writeLock().lock();

    try {
      List<BatchOperation<byte[], byte[]>> operations = new ArrayList<>(entries.size() + 1);

      long expectedIndex = lastLogIndex;

      for (RaftLogEntry entry : entries) {
        Objects.requireNonNull(entry, "Log entry must not be null");

        if (expectedIndex == Long.MAX_VALUE) {
          throw new IllegalStateException("Raft log index space exhausted");
        }

        expectedIndex++;

        if (entry.getIndex() != expectedIndex) {
          throw new IllegalArgumentException(
              "Expected log index " + expectedIndex + " but received " + entry.getIndex());
        }

        operations.add(BatchOperation.put(entryKey(entry.getIndex()), serializer.serialize(entry)));
      }

      operations.add(BatchOperation.put(LAST_INDEX_KEY, encodeLong(expectedIndex)));

      store.writeBatch(operations, true);
      lastLogIndex = expectedIndex;

    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to append Raft log entries", exception);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Optional<RaftLogEntry> getEntry(long index) {
    validateIndex(index);

    lock.readLock().lock();

    try {
      return Optional.ofNullable(readEntry(index));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
    validateIndex(fromIndex);

    if (toIndex < fromIndex) {
      throw new IllegalArgumentException("End index must not be less than start index");
    }

    lock.readLock().lock();

    try {
      if (toIndex > lastLogIndex) {
        throw new IllegalArgumentException("Requested range exceeds the last log index");
      }

      List<RaftLogEntry> entries = new ArrayList<>();

      for (long index = fromIndex; ; index++) {
        RaftLogEntry entry = readEntry(index);

        if (entry == null) {
          throw new RaftLogException("Missing Raft log entry at index " + index);
        }

        entries.add(entry);

        if (index == toIndex) {
          break;
        }
      }

      return List.copyOf(entries);

    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long getLastLogIndex() {
    lock.readLock().lock();

    try {
      return lastLogIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long getLastLogTerm() {
    lock.readLock().lock();

    try {
      if (lastLogIndex == 0) {
        return 0;
      }

      RaftLogEntry entry = readEntry(lastLogIndex);

      if (entry == null) {
        throw new RaftLogException("Last Raft log entry is missing");
      }

      return entry.getTerm();

    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void truncateSuffix(long fromIndex) {
    validateIndex(fromIndex);

    lock.writeLock().lock();

    try {
      if (fromIndex > lastLogIndex) {
        return;
      }

      List<BatchOperation<byte[], byte[]>> operations = new ArrayList<>();

      for (long index = fromIndex; ; index++) {
        operations.add(BatchOperation.delete(entryKey(index)));

        if (index == lastLogIndex) {
          break;
        }
      }

      long newLastIndex = fromIndex - 1;

      operations.add(BatchOperation.put(LAST_INDEX_KEY, encodeLong(newLastIndex)));

      store.writeBatch(operations, true);
      lastLogIndex = newLastIndex;

    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to truncate Raft log suffix", exception);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void flush() {
    try {
      store.flush();
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to flush Raft log", exception);
    }
  }

  @Override
  public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
    validateIndex(fromIndex);
    Objects.requireNonNull(entries, "Entries must not be null");

    lock.writeLock().lock();
    try {
      if (fromIndex > lastLogIndex && fromIndex - lastLogIndex > 1) {
        throw new IllegalArgumentException("Replacement index cannot create a gap in the Raft log");
      }

      List<BatchOperation<byte[], byte[]>> operations = new ArrayList<>();

      // Delete the existing suffix.
      for (long index = fromIndex; index <= lastLogIndex && index > 0; index++) {
        operations.add(BatchOperation.delete(entryKey(index)));

        if (index == Long.MAX_VALUE) {
          break;
        }
      }

      // Add replacement entries, ensuring consecutive indexes.
      long expectedIndex = fromIndex;

      for (RaftLogEntry entry : entries) {
        Objects.requireNonNull(entry, "Log entry must not be null");

        if (entry.getIndex() != expectedIndex) {
          throw new IllegalArgumentException(
              "Expected log entry at index " + expectedIndex + " but received " + entry.getIndex());
        }

        operations.add(BatchOperation.put(entryKey(entry.getIndex()), serializer.serialize(entry)));

        if (expectedIndex == Long.MAX_VALUE && entry != entries.get(entries.size() - 1)) {
          throw new IllegalArgumentException("Raft log index overflow");
        }

        expectedIndex++;
      }

      long newLastLogIndex =
          entries.isEmpty() ? fromIndex - 1 : entries.get(entries.size() - 1).getIndex();

      operations.add(BatchOperation.put(LAST_INDEX_KEY, encodeLong(newLastLogIndex)));

      // All deletions, replacements, and metadata updates are atomic.
      store.writeBatch(operations, true);

      lastLogIndex = newLastLogIndex;
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to replace Raft log suffix", exception);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public RaftMetadata loadMetadata() {
    lock.readLock().lock();
    try {
      byte[] data = store.get(RAFT_METADATA_KEY);

      if (data == null) {
        return new RaftMetadata(0, null);
      }

      return deserializeMetadata(data);
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to load Raft metadata", exception);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void saveMetadata(RaftMetadata metadata) {
    Objects.requireNonNull(metadata, "Raft metadata must not be null");

    lock.writeLock().lock();
    try {
      byte[] serialized = serializeMetadata(metadata);

      store.writeBatch(List.of(BatchOperation.put(RAFT_METADATA_KEY, serialized)), true);
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to save Raft metadata", exception);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static byte[] serializeMetadata(RaftMetadata metadata) {
    byte[] votedFor =
        metadata.getVotedFor().map(value -> value.getBytes(StandardCharsets.UTF_8)).orElse(null);

    int votedForLength = votedFor == null ? -1 : votedFor.length;

    ByteBuffer buffer =
        ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Math.max(votedForLength, 0));

    buffer.putLong(metadata.getCurrentTerm());
    buffer.putInt(votedForLength);

    if (votedFor != null) {
      buffer.put(votedFor);
    }

    return buffer.array();
  }

  private static RaftMetadata deserializeMetadata(byte[] data) {
    if (data.length < Long.BYTES + Integer.BYTES) {
      throw new RaftLogException("Corrupted Raft metadata: incomplete data");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);

    long currentTerm = buffer.getLong();
    int votedForLength = buffer.getInt();

    if (votedForLength < -1) {
      throw new RaftLogException("Corrupted Raft metadata: invalid candidate length");
    }

    if (votedForLength == -1 && buffer.hasRemaining()) {
      throw new RaftLogException("Corrupted Raft metadata: unexpected candidate data");
    }

    if (votedForLength >= 0 && votedForLength != buffer.remaining()) {
      throw new RaftLogException("Corrupted Raft metadata: invalid candidate length");
    }

    String votedFor = null;

    if (votedForLength >= 0) {
      byte[] candidateBytes = new byte[votedForLength];
      buffer.get(candidateBytes);
      votedFor = new String(candidateBytes, StandardCharsets.UTF_8);
    }

    try {
      return new RaftMetadata(currentTerm, votedFor);
    } catch (IllegalArgumentException exception) {
      throw new RaftLogException("Corrupted Raft metadata", exception);
    }
  }

  private RaftLogEntry readEntry(long index) {
    try {
      byte[] data = store.get(entryKey(index));

      return data == null ? null : serializer.deserialize(data);

    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to read Raft log entry " + index, exception);
    }
  }

  private long loadLastLogIndex() {
    try {
      byte[] data = store.get(LAST_INDEX_KEY);

      if (data == null) {
        return 0;
      }

      if (data.length != Long.BYTES) {
        throw new RaftLogException("Invalid persisted last log index metadata");
      }

      long index = ByteBuffer.wrap(data).getLong();

      if (index < 0) {
        throw new RaftLogException("Persisted last log index cannot be negative");
      }

      return index;

    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to recover Raft log metadata", exception);
    }
  }

  private static byte[] entryKey(long index) {
    ByteBuffer buffer = ByteBuffer.allocate(ENTRY_PREFIX.length + Long.BYTES);
    buffer.put(ENTRY_PREFIX);
    buffer.putLong(index);
    return buffer.array();
  }

  private static byte[] encodeLong(long value) {
    return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
  }

  private static void validateIndex(long index) {
    if (index <= 0) {
      throw new IllegalArgumentException("Log index must be positive");
    }
  }

  private void validateRecoveredLog() {
    for (long index = 1; index <= lastLogIndex; index++) {
      RaftLogEntry entry = readEntry(index);

      if (entry == null) {
        throw new RaftLogException("Raft log contains a missing entry at index " + index);
      }

      if (entry.getIndex() != index) {
        throw new RaftLogException(
            "Raft log entry index mismatch: expected " + index + " but found " + entry.getIndex());
      }

      if (index == Long.MAX_VALUE) {
        break;
      }
    }
  }

  @Override
  public void close() {
    store.close();
  }
}
