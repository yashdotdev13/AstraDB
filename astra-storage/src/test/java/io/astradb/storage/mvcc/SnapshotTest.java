package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import org.junit.jupiter.api.Test;

class SnapshotTest {

  @Test
  void shouldCreateSnapshot() {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0);

    Snapshot snapshot = new Snapshot(transactionId, timestamp);

    assertEquals(transactionId, snapshot.getTransactionId());
    assertEquals(timestamp, snapshot.getTimestamp());
  }

  @Test
  void shouldRejectNullTransactionId() {
    assertThrows(
        NullPointerException.class, () -> new Snapshot(null, new HybridTimestamp(1000L, 0)));
  }

  @Test
  void shouldRejectNullTimestamp() {
    assertThrows(NullPointerException.class, () -> new Snapshot(TransactionId.generate(), null));
  }

  @Test
  void shouldImplementEquality() {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0);

    Snapshot first = new Snapshot(transactionId, timestamp);
    Snapshot second = new Snapshot(transactionId, timestamp);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void shouldNotBeEqualWhenTransactionIdsDiffer() {
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0);

    Snapshot first = new Snapshot(TransactionId.generate(), timestamp);
    Snapshot second = new Snapshot(TransactionId.generate(), timestamp);

    assertNotEquals(first, second);
  }

  @Test
  void shouldNotBeEqualWhenTimestampsDiffer() {
    TransactionId transactionId = TransactionId.generate();

    Snapshot first = new Snapshot(transactionId, new HybridTimestamp(1000L, 0));
    Snapshot second = new Snapshot(transactionId, new HybridTimestamp(2000L, 0));

    assertNotEquals(first, second);
  }

  @Test
  void shouldImplementToString() {
    Snapshot snapshot = new Snapshot(TransactionId.generate(), new HybridTimestamp(1000L, 0));

    assertNotNull(snapshot.toString());
    assertTrue(snapshot.toString().contains("Snapshot"));
  }
}
