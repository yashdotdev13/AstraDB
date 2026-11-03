package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestamp;
import org.junit.jupiter.api.Test;

class VisibilityCheckerTest {

  @Test
  void versionBeforeSnapshotIsVisible() {
    VersionedValue<String> version = VersionedValue.of(new HybridTimestamp(100L, 0L), "value");
    HybridTimestamp snapshot = new HybridTimestamp(200L, 0L);

    assertTrue(VisibilityChecker.isVisible(version, snapshot));
  }

  @Test
  void versionAtSnapshotTimestampIsVisible() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 5L);
    VersionedValue<String> version = VersionedValue.of(timestamp, "value");

    assertTrue(VisibilityChecker.isVisible(version, timestamp));
  }

  @Test
  void versionAfterSnapshotIsNotVisible() {
    VersionedValue<String> version = VersionedValue.of(new HybridTimestamp(200L, 0L), "value");
    HybridTimestamp snapshot = new HybridTimestamp(100L, 0L);

    assertFalse(VisibilityChecker.isVisible(version, snapshot));
  }

  @Test
  void logicalCounterDeterminesVisibilityAtSamePhysicalTime() {
    VersionedValue<String> version = VersionedValue.of(new HybridTimestamp(100L, 5L), "value");
    HybridTimestamp snapshot = new HybridTimestamp(100L, 4L);

    assertFalse(VisibilityChecker.isVisible(version, snapshot));
  }

  @Test
  void tombstoneCanBeVisibleToSnapshot() {
    VersionedValue<String> tombstone = VersionedValue.tombstone(new HybridTimestamp(100L, 0L));
    HybridTimestamp snapshot = new HybridTimestamp(100L, 0L);

    assertTrue(VisibilityChecker.isVisible(tombstone, snapshot));
  }

  @Test
  void rejectsNullVersion() {
    HybridTimestamp snapshot = new HybridTimestamp(100L, 0L);

    assertThrows(NullPointerException.class, () -> VisibilityChecker.isVisible(null, snapshot));
  }

  @Test
  void rejectsNullSnapshotTimestamp() {
    VersionedValue<String> version = VersionedValue.of(new HybridTimestamp(100L, 0L), "value");

    assertThrows(NullPointerException.class, () -> VisibilityChecker.isVisible(version, null));
  }
}
