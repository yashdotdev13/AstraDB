package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotManagerTest {

  private SnapshotManager snapshotManager;

  @BeforeEach
  void setUp() {
    snapshotManager = new SnapshotManager();
  }

  private Snapshot createSnapshot(long physicalTime) {
    return new Snapshot(TransactionId.generate(), new HybridTimestamp(physicalTime, 0));
  }

  @Test
  void shouldBeEmptyInitially() {
    assertTrue(snapshotManager.isEmpty());
    assertEquals(0, snapshotManager.size());
    assertEquals(Optional.empty(), snapshotManager.getOldestActiveTimestamp());
  }

  @Test
  void shouldRegisterSnapshot() {
    Snapshot snapshot = createSnapshot(1000L);

    snapshotManager.register(snapshot);

    assertFalse(snapshotManager.isEmpty());
    assertEquals(1, snapshotManager.size());
    assertEquals(Optional.of(snapshot), snapshotManager.get(snapshot.getTransactionId()));
  }

  @Test
  void shouldRejectNullSnapshot() {
    assertThrows(NullPointerException.class, () -> snapshotManager.register(null));
  }

  @Test
  void shouldRejectDuplicateTransactionRegistration() {
    Snapshot snapshot = createSnapshot(1000L);

    snapshotManager.register(snapshot);

    Snapshot duplicate = new Snapshot(snapshot.getTransactionId(), new HybridTimestamp(2000L, 0));

    assertThrows(IllegalArgumentException.class, () -> snapshotManager.register(duplicate));
    assertEquals(1, snapshotManager.size());
    assertEquals(Optional.of(snapshot), snapshotManager.get(snapshot.getTransactionId()));
  }

  @Test
  void shouldReturnEmptyForUnknownTransaction() {
    assertEquals(Optional.empty(), snapshotManager.get(TransactionId.generate()));
  }

  @Test
  void shouldRejectNullTransactionIdWhenGettingSnapshot() {
    assertThrows(NullPointerException.class, () -> snapshotManager.get(null));
  }

  @Test
  void shouldReleaseRegisteredSnapshot() {
    Snapshot snapshot = createSnapshot(1000L);
    snapshotManager.register(snapshot);

    assertTrue(snapshotManager.release(snapshot.getTransactionId()));

    assertTrue(snapshotManager.isEmpty());
    assertEquals(0, snapshotManager.size());
    assertEquals(Optional.empty(), snapshotManager.get(snapshot.getTransactionId()));
  }

  @Test
  void shouldReturnFalseWhenReleasingUnknownTransaction() {
    assertFalse(snapshotManager.release(TransactionId.generate()));
  }

  @Test
  void shouldRejectNullTransactionIdWhenReleasingSnapshot() {
    assertThrows(NullPointerException.class, () -> snapshotManager.release(null));
  }

  @Test
  void shouldFindOldestActiveSnapshotTimestamp() {
    Snapshot snapshot1 = createSnapshot(3000L);
    Snapshot snapshot2 = createSnapshot(1000L);
    Snapshot snapshot3 = createSnapshot(2000L);

    snapshotManager.register(snapshot1);
    snapshotManager.register(snapshot2);
    snapshotManager.register(snapshot3);

    assertEquals(Optional.of(snapshot2.getTimestamp()), snapshotManager.getOldestActiveTimestamp());
  }

  @Test
  void shouldUpdateOldestTimestampAfterReleasingSnapshot() {
    Snapshot snapshot1 = createSnapshot(1000L);
    Snapshot snapshot2 = createSnapshot(2000L);

    snapshotManager.register(snapshot1);
    snapshotManager.register(snapshot2);

    snapshotManager.release(snapshot1.getTransactionId());

    assertEquals(Optional.of(snapshot2.getTimestamp()), snapshotManager.getOldestActiveTimestamp());
  }

  @Test
  void shouldReturnEmptyOldestTimestampAfterReleasingAllSnapshots() {
    Snapshot snapshot = createSnapshot(1000L);
    snapshotManager.register(snapshot);

    snapshotManager.release(snapshot.getTransactionId());

    assertEquals(Optional.empty(), snapshotManager.getOldestActiveTimestamp());
  }
}
