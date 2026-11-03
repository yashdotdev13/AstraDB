package io.astradb.storage.mvcc;

import io.astradb.common.time.HybridTimestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Maintains the ordered versions of a single key in the MVCC storage engine.
 *
 * <p>Versions are stored in descending timestamp order, with the newest version first.
 *
 * @param <V> type of the stored value
 */
public final class VersionChain<V> {

  private final List<VersionedValue<V>> versions = new ArrayList<>();

  /**
   * Adds a version to this chain.
   *
   * @param version version to add
   * @throws NullPointerException if version is null
   * @throws IllegalArgumentException if a version with the same timestamp already exists
   */
  public synchronized void add(VersionedValue<V> version) {
    Objects.requireNonNull(version, "Version must not be null");

    HybridTimestamp timestamp = version.getTimestamp();

    for (int i = 0; i < versions.size(); i++) {
      int comparison = versions.get(i).getTimestamp().compareTo(timestamp);

      if (comparison == 0) {
        throw new IllegalArgumentException("Duplicate version timestamp: " + timestamp);
      }

      if (comparison < 0) {
        versions.add(i, version);
        return;
      }
    }

    versions.add(version);
  }

  /**
   * Returns the latest version visible to the specified snapshot.
   *
   * <p>If the latest visible version is a tombstone, the key is considered deleted.
   *
   * @param snapshotTimestamp snapshot timestamp
   * @return visible value, or empty if no visible value exists or it was deleted
   */
  public synchronized Optional<V> get(HybridTimestamp snapshotTimestamp) {
    Objects.requireNonNull(snapshotTimestamp, "Snapshot timestamp must not be null");

    for (VersionedValue<V> version : versions) {
      if (VisibilityChecker.isVisible(version, snapshotTimestamp)) {
        return version.isTombstone() ? Optional.empty() : Optional.of(version.getValue());
      }
    }

    return Optional.empty();
  }

  /**
   * Returns an immutable copy of all versions, newest first.
   *
   * @return unmodifiable version list
   */
  public synchronized List<VersionedValue<V>> getVersions() {
    return Collections.unmodifiableList(new ArrayList<>(versions));
  }

  /**
   * Returns the number of versions in this chain.
   *
   * @return version count
   */
  public synchronized int size() {
    return versions.size();
  }

  /**
   * Returns whether this chain contains no versions.
   *
   * @return true if empty
   */
  public synchronized boolean isEmpty() {
    return versions.isEmpty();
  }
}
