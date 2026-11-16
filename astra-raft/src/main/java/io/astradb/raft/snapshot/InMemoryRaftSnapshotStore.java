package io.astradb.raft.snapshot;

import java.util.Objects;
import java.util.Optional;

/** In-memory implementation of the Raft snapshot store. */
public final class InMemoryRaftSnapshotStore implements RaftSnapshotStore {

  private RaftSnapshot snapshot;
  private boolean closed;

  @Override
  public synchronized void save(RaftSnapshot snapshot) {
    ensureOpen();

    Objects.requireNonNull(snapshot, "Snapshot must not be null");

    if (this.snapshot != null
        && snapshot.getLastIncludedIndex() < this.snapshot.getLastIncludedIndex()) {
      throw new IllegalArgumentException("Cannot replace a newer snapshot with an older snapshot");
    }

    this.snapshot = snapshot;
  }

  @Override
  public synchronized Optional<RaftSnapshot> load() {
    ensureOpen();
    return Optional.ofNullable(snapshot);
  }

  @Override
  public synchronized void delete() {
    ensureOpen();
    snapshot = null;
  }

  @Override
  public synchronized void close() {
    closed = true;
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Raft snapshot store is closed");
    }
  }
}
