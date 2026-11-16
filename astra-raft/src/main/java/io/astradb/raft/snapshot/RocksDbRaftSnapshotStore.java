package io.astradb.raft.snapshot;

import io.astradb.raft.log.RaftLogException;
import io.astradb.storage.rocksdb.RocksDbStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.rocksdb.RocksDBException;

/** RocksDB-backed persistent storage for Raft snapshots. */
public final class RocksDbRaftSnapshotStore implements RaftSnapshotStore {

  private static final byte[] SNAPSHOT_KEY =
      "raft:snapshot:latest".getBytes(java.nio.charset.StandardCharsets.UTF_8);

  private static final int FORMAT_VERSION = 1;

  private final RocksDbStore store;
  private boolean closed;

  private RocksDbRaftSnapshotStore(RocksDbStore store) {
    this.store = Objects.requireNonNull(store);
    validateStoredSnapshot();
  }

  /** Opens or creates a persistent Raft snapshot database. */
  public static RocksDbRaftSnapshotStore open(Path databasePath) {
    Objects.requireNonNull(databasePath, "Database path must not be null");

    try {
      return new RocksDbRaftSnapshotStore(RocksDbStore.open(databasePath));
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to open Raft snapshot database", exception);
    }
  }

  @Override
  public synchronized void save(RaftSnapshot snapshot) {
    ensureOpen();
    Objects.requireNonNull(snapshot, "Snapshot must not be null");

    Optional<RaftSnapshot> existing = load();

    if (existing.isPresent()
        && snapshot.getLastIncludedIndex() < existing.get().getLastIncludedIndex()) {
      throw new IllegalArgumentException("Cannot replace a newer snapshot with an older snapshot");
    }

    try {
      store.put(SNAPSHOT_KEY, serialize(snapshot));
      store.flush();
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to persist Raft snapshot", exception);
    }
  }

  @Override
  public synchronized Optional<RaftSnapshot> load() {
    ensureOpen();

    try {
      byte[] data = store.get(SNAPSHOT_KEY);
      return data == null ? Optional.empty() : Optional.of(deserialize(data));
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to load Raft snapshot", exception);
    }
  }

  @Override
  public synchronized void delete() {
    ensureOpen();

    try {
      store.delete(SNAPSHOT_KEY);
      store.flush();
    } catch (RocksDBException exception) {
      throw new RaftLogException("Failed to delete Raft snapshot", exception);
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      store.close();
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Raft snapshot store is closed");
    }
  }

  private void validateStoredSnapshot() {
    try {
      byte[] data = store.get(SNAPSHOT_KEY);
      if (data != null) {
        deserialize(data);
      }
    } catch (RocksDBException exception) {
      store.close();
      throw new RaftLogException("Failed to validate persisted Raft snapshot", exception);
    } catch (RuntimeException exception) {
      store.close();
      throw exception;
    }
  }

  private byte[] serialize(RaftSnapshot snapshot) {
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteStream)) {

      byte[] data = snapshot.getData();

      output.writeInt(FORMAT_VERSION);
      output.writeLong(snapshot.getLastIncludedIndex());
      output.writeLong(snapshot.getLastIncludedTerm());
      output.writeLong(snapshot.getCreatedAt().getEpochSecond());
      output.writeInt(snapshot.getCreatedAt().getNano());
      output.writeInt(data.length);
      output.write(data);

      return byteStream.toByteArray();
    } catch (IOException exception) {
      throw new RaftLogException("Failed to serialize Raft snapshot", exception);
    }
  }

  private RaftSnapshot deserialize(byte[] serialized) {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {

      int version = input.readInt();

      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported Raft snapshot format version: " + version);
      }

      long index = input.readLong();
      long term = input.readLong();
      long epochSecond = input.readLong();
      int nano = input.readInt();
      int dataLength = input.readInt();

      if (dataLength < 0 || dataLength != input.available()) {
        throw new IOException("Invalid Raft snapshot data length");
      }

      byte[] data = new byte[dataLength];
      input.readFully(data);

      if (input.available() != 0) {
        throw new IOException("Unexpected trailing Raft snapshot data");
      }

      return new RaftSnapshot(index, term, data, Instant.ofEpochSecond(epochSecond, nano));

    } catch (IOException | RuntimeException exception) {
      throw new RaftLogException("Corrupted persisted Raft snapshot", exception);
    }
  }
}
