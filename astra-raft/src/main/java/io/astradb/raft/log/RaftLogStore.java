package io.astradb.raft.log;

import java.util.List;
import java.util.Optional;

/** Defines persistent storage operations for the Raft replicated log. */
public interface RaftLogStore extends AutoCloseable {

  /** Appends the next entry to the log. */
  void append(RaftLogEntry entry);

  /** Atomically appends a sequence of consecutive entries. */
  void appendAll(List<RaftLogEntry> entries);

  /** Retrieves an entry by its log index. */
  Optional<RaftLogEntry> getEntry(long index);

  /** Retrieves entries in the inclusive range [fromIndex, toIndex]. */
  List<RaftLogEntry> getEntries(long fromIndex, long toIndex);

  /** Returns the last log index, or zero when the log is empty. */
  long getLastLogIndex();

  /** Returns the term of the last log entry, or zero when the log is empty. */
  long getLastLogTerm();

  /** Removes all entries whose indexes are greater than or equal to fromIndex. */
  void truncateSuffix(long fromIndex);

  /** Flushes pending writes. */
  void flush();

  void replaceSuffix(long fromIndex, List<RaftLogEntry> entries);

  /** Returns the first log index available after the snapshot boundary. */
  default long getFirstLogIndex() {
    return getLastIncludedIndex() + 1;
  }

  /** Returns the last index included in the snapshot, or zero. */
  default long getLastIncludedIndex() {
    return 0;
  }

  /** Returns the term of the last entry included in the snapshot, or zero. */
  default long getLastIncludedTerm() {
    return 0;
  }

  /** Returns the term at an index, including the snapshot boundary. */
  default Optional<Long> getTerm(long index) {
    if (index == getLastIncludedIndex() && index > 0) {
      return Optional.of(getLastIncludedTerm());
    }

    return getEntry(index).map(RaftLogEntry::getTerm);
  }

  /** Removes log entries up to and including the supplied snapshot index. */
  default void compactPrefix(long lastIncludedIndex, long lastIncludedTerm) {
    throw new UnsupportedOperationException("Log prefix compaction is not implemented");
  }

  /**
   * Installs a snapshot boundary received from another Raft node.
   *
   * <p>Preserves the suffix only when the local entry at the snapshot index has the same term.
   * Otherwise, discards the retained log.
   */
  default void installSnapshotBoundary(long lastIncludedIndex, long lastIncludedTerm) {
    throw new UnsupportedOperationException("Snapshot boundary installation is not implemented");
  }

  @Override
  void close();
}
