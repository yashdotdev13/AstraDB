package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VersionChainTest {

  private VersionChain<String> versionChain;

  private HybridTimestamp timestamp1;
  private HybridTimestamp timestamp2;
  private HybridTimestamp timestamp3;

  @BeforeEach
  void setUp() {
    versionChain = new VersionChain<>();

    timestamp1 = new HybridTimestamp(1000L, 0);
    timestamp2 = new HybridTimestamp(2000L, 0);
    timestamp3 = new HybridTimestamp(3000L, 0);
  }

  @Test
  void shouldBeEmptyInitially() {
    assertTrue(versionChain.isEmpty());
    assertEquals(0, versionChain.size());
    assertEquals(Optional.empty(), versionChain.get(timestamp1));
  }

  @Test
  void shouldAddSingleVersion() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));

    assertFalse(versionChain.isEmpty());
    assertEquals(1, versionChain.size());
    assertEquals(Optional.of("value-1"), versionChain.get(timestamp1));
  }

  @Test
  void shouldMaintainVersionsInDescendingTimestampOrder() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));
    versionChain.add(VersionedValue.of(timestamp3, "value-3"));
    versionChain.add(VersionedValue.of(timestamp2, "value-2"));

    List<VersionedValue<String>> versions = versionChain.getVersions();

    assertEquals(3, versions.size());
    assertEquals(timestamp3, versions.get(0).getTimestamp());
    assertEquals(timestamp2, versions.get(1).getTimestamp());
    assertEquals(timestamp1, versions.get(2).getTimestamp());
  }

  @Test
  void shouldReturnLatestVisibleVersionForSnapshot() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));
    versionChain.add(VersionedValue.of(timestamp2, "value-2"));
    versionChain.add(VersionedValue.of(timestamp3, "value-3"));

    assertEquals(Optional.empty(), versionChain.get(new HybridTimestamp(500L, 0)));
    assertEquals(Optional.of("value-1"), versionChain.get(timestamp1));
    assertEquals(Optional.of("value-2"), versionChain.get(timestamp2));
    assertEquals(Optional.of("value-3"), versionChain.get(timestamp3));
    assertEquals(Optional.of("value-2"), versionChain.get(new HybridTimestamp(2500L, 0)));
  }

  @Test
  void shouldReturnEmptyWhenLatestVisibleVersionIsTombstone() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));
    versionChain.add(VersionedValue.tombstone(timestamp2));

    assertEquals(Optional.of("value-1"), versionChain.get(timestamp1));
    assertEquals(Optional.empty(), versionChain.get(timestamp2));
    assertEquals(Optional.empty(), versionChain.get(timestamp3));
  }

  @Test
  void shouldAllowValueToBeRestoredAfterTombstone() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));
    versionChain.add(VersionedValue.tombstone(timestamp2));
    versionChain.add(VersionedValue.of(timestamp3, "value-3"));

    assertEquals(Optional.of("value-1"), versionChain.get(timestamp1));
    assertEquals(Optional.empty(), versionChain.get(timestamp2));
    assertEquals(Optional.of("value-3"), versionChain.get(timestamp3));
  }

  @Test
  void shouldRejectDuplicateTimestamps() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));

    assertThrows(
        IllegalArgumentException.class,
        () -> versionChain.add(VersionedValue.of(timestamp1, "value-2")));
  }

  @Test
  void shouldRejectNullVersion() {
    assertThrows(NullPointerException.class, () -> versionChain.add(null));
  }

  @Test
  void shouldRejectNullSnapshotTimestamp() {
    assertThrows(NullPointerException.class, () -> versionChain.get(null));
  }

  @Test
  void shouldReturnImmutableVersionList() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));

    List<VersionedValue<String>> versions = versionChain.getVersions();

    assertThrows(
        UnsupportedOperationException.class,
        () -> versions.add(VersionedValue.of(timestamp2, "value-2")));

    assertEquals(1, versionChain.size());
  }

  @Test
  void shouldReturnDefensiveCopyOfVersions() {
    versionChain.add(VersionedValue.of(timestamp1, "value-1"));

    List<VersionedValue<String>> versions = versionChain.getVersions();

    versionChain.add(VersionedValue.of(timestamp2, "value-2"));

    assertEquals(1, versions.size());
    assertEquals(2, versionChain.getVersions().size());
  }
}
