package io.astradb.raft.snapshot;

import java.util.Optional;

/** Defines persistence operations for Raft state machine snapshots. */
public interface RaftSnapshotStore extends AutoCloseable {

  /** Persists the latest Raft snapshot. */
  void save(RaftSnapshot snapshot);

  /** Retrieves the latest persisted snapshot. */
  Optional<RaftSnapshot> load();

  /** Deletes the persisted snapshot, if present. */
  void delete();

  @Override
  void close();
}
