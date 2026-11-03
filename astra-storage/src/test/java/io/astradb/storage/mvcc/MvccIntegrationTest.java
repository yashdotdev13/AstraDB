package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MvccIntegrationTest {

  private VersionChain<String> accountBalance;
  private SnapshotManager snapshotManager;

  @BeforeEach
  void setUp() {
    accountBalance = new VersionChain<>();
    snapshotManager = new SnapshotManager();
  }

  private HybridTimestamp timestamp(long physicalTime) {
    return new HybridTimestamp(physicalTime, 0);
  }

  @Test
  void shouldProvideConsistentReadsAcrossConcurrentSnapshots() {
    // Initial committed value.
    accountBalance.add(VersionedValue.of(timestamp(1000L), "100"));

    // Transaction A starts and captures its snapshot.
    Snapshot transactionA = new Snapshot(TransactionId.generate(), timestamp(1000L));
    snapshotManager.register(transactionA);

    // A later transaction writes a new version.
    accountBalance.add(VersionedValue.of(timestamp(2000L), "200"));

    // Transaction B starts after the new version.
    Snapshot transactionB = new Snapshot(TransactionId.generate(), timestamp(2000L));
    snapshotManager.register(transactionB);

    // A continues to see its original snapshot.
    assertEquals(Optional.of("100"), accountBalance.get(transactionA.getTimestamp()));

    // B sees the latest committed version.
    assertEquals(Optional.of("200"), accountBalance.get(transactionB.getTimestamp()));

    assertEquals(2, snapshotManager.size());
    assertEquals(
        Optional.of(transactionA.getTimestamp()), snapshotManager.getOldestActiveTimestamp());
  }

  @Test
  void shouldPreserveSnapshotVisibilityAfterDeletion() {
    accountBalance.add(VersionedValue.of(timestamp(1000L), "active"));

    Snapshot beforeDelete = new Snapshot(TransactionId.generate(), timestamp(1000L));
    snapshotManager.register(beforeDelete);

    // Delete the key at a later timestamp.
    accountBalance.add(VersionedValue.tombstone(timestamp(2000L)));

    Snapshot afterDelete = new Snapshot(TransactionId.generate(), timestamp(2000L));
    snapshotManager.register(afterDelete);

    assertEquals(Optional.of("active"), accountBalance.get(beforeDelete.getTimestamp()));

    assertEquals(Optional.empty(), accountBalance.get(afterDelete.getTimestamp()));
  }

  @Test
  void shouldSupportKeyRecreationAfterDeletion() {
    accountBalance.add(VersionedValue.of(timestamp(1000L), "first"));
    accountBalance.add(VersionedValue.tombstone(timestamp(2000L)));
    accountBalance.add(VersionedValue.of(timestamp(3000L), "recreated"));

    assertEquals(Optional.of("first"), accountBalance.get(timestamp(1000L)));
    assertEquals(Optional.empty(), accountBalance.get(timestamp(2000L)));
    assertEquals(Optional.of("recreated"), accountBalance.get(timestamp(3000L)));
  }

  @Test
  void shouldTrackOldestSnapshotThroughoutTransactionLifecycle() {
    Snapshot transactionA = new Snapshot(TransactionId.generate(), timestamp(1000L));
    Snapshot transactionB = new Snapshot(TransactionId.generate(), timestamp(2000L));
    Snapshot transactionC = new Snapshot(TransactionId.generate(), timestamp(3000L));

    snapshotManager.register(transactionA);
    snapshotManager.register(transactionB);
    snapshotManager.register(transactionC);

    assertEquals(Optional.of(timestamp(1000L)), snapshotManager.getOldestActiveTimestamp());

    // Transaction A completes.
    snapshotManager.release(transactionA.getTransactionId());

    assertEquals(Optional.of(timestamp(2000L)), snapshotManager.getOldestActiveTimestamp());

    // Transaction B completes.
    snapshotManager.release(transactionB.getTransactionId());

    assertEquals(Optional.of(timestamp(3000L)), snapshotManager.getOldestActiveTimestamp());

    // All transactions complete.
    snapshotManager.release(transactionC.getTransactionId());

    assertTrue(snapshotManager.isEmpty());
    assertEquals(Optional.empty(), snapshotManager.getOldestActiveTimestamp());
  }

  @Test
  void shouldMaintainIndependentVersionChainsForDifferentKeys() {
    VersionChain<String> userName = new VersionChain<>();
    VersionChain<String> userEmail = new VersionChain<>();

    userName.add(VersionedValue.of(timestamp(1000L), "Alice"));
    userEmail.add(VersionedValue.of(timestamp(1000L), "alice@example.com"));

    userName.add(VersionedValue.of(timestamp(2000L), "Bob"));

    Snapshot snapshot = new Snapshot(TransactionId.generate(), timestamp(1500L));

    assertEquals(Optional.of("Alice"), userName.get(snapshot.getTimestamp()));
    assertEquals(Optional.of("alice@example.com"), userEmail.get(snapshot.getTimestamp()));

    assertEquals(Optional.of("Bob"), userName.get(timestamp(2000L)));
    assertEquals(Optional.of("alice@example.com"), userEmail.get(timestamp(2000L)));
  }
}
