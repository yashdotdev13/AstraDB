package io.astradb.storage.mvcc;

import io.astradb.common.time.HybridTimestamp;
import java.util.Objects;

/** Determines whether a version is visible to a given MVCC snapshot. */
public final class VisibilityChecker {

  private VisibilityChecker() {}

  /**
   * Checks whether a version's timestamp is at or before the snapshot timestamp.
   *
   * @param version version to evaluate
   * @param snapshotTimestamp timestamp defining the snapshot
   * @return true if the version is visible to the snapshot
   */
  public static boolean isVisible(VersionedValue<?> version, HybridTimestamp snapshotTimestamp) {

    Objects.requireNonNull(version, "Version must not be null");
    Objects.requireNonNull(snapshotTimestamp, "Snapshot timestamp must not be null");

    return version.getTimestamp().compareTo(snapshotTimestamp) <= 0;
  }
}
