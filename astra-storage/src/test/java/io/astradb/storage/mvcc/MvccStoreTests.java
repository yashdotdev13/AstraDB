package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.wal.LogSequenceNumber;
import io.astradb.storage.wal.WalRecord;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MvccStoreTests {

  private MvccStore<String, String> store;

  @BeforeEach
  void setUp() {
    store = new MvccStore<>();
  }

  @Test
  void shouldReturnEmptyForMissingKey() {
    assertTrue(store.get("missing", timestamp(10)).isEmpty());
  }

  @Test
  void shouldStoreAndRetrieveValue() {
    store.put("key", "value", timestamp(10));

    assertEquals("value", store.get("key", timestamp(10)).orElseThrow());
  }

  @Test
  void shouldReturnLatestVisibleVersion() {
    store.put("key", "v1", timestamp(10));
    store.put("key", "v2", timestamp(20));

    assertEquals("v1", store.get("key", timestamp(15)).orElseThrow());
    assertEquals("v2", store.get("key", timestamp(20)).orElseThrow());
  }

  @Test
  void shouldNotExposeFutureVersions() {
    store.put("key", "future", timestamp(20));

    assertTrue(store.get("key", timestamp(10)).isEmpty());
  }

  @Test
  void shouldReturnEmptyAfterDeletion() {
    store.put("key", "value", timestamp(10));
    store.delete("key", timestamp(20));

    assertTrue(store.get("key", timestamp(20)).isEmpty());
  }

  @Test
  void shouldReturnPreviousValueForSnapshotBeforeDeletion() {
    store.put("key", "value", timestamp(10));
    store.delete("key", timestamp(20));

    assertEquals("value", store.get("key", timestamp(15)).orElseThrow());
  }

  @Test
  void shouldTrackNumberOfKeys() {
    store.put("key1", "value1", timestamp(10));
    store.put("key2", "value2", timestamp(20));

    assertEquals(2, store.size());
    assertFalse(store.isEmpty());
  }

  @Test
  void shouldClearAllData() {
    store.put("key", "value", timestamp(10));

    store.clear();

    assertEquals(0, store.size());
    assertTrue(store.isEmpty());
    assertTrue(store.get("key", timestamp(10)).isEmpty());
  }

  @Test
  void shouldRejectNullKey() {
    assertThrows(NullPointerException.class, () -> store.put(null, "value", timestamp(10)));
  }

  @Test
  void shouldRejectNullValue() {
    assertThrows(NullPointerException.class, () -> store.put("key", null, timestamp(10)));
  }

  @Test
  void shouldRejectNullTimestamp() {
    assertThrows(NullPointerException.class, () -> store.put("key", "value", null));
  }

  @Test
  void shouldRecoverPutRecord() {
    TransactionId transactionId = TransactionId.generate();

    List<WalRecord> records =
        List.of(
            WalRecord.begin(lsn(0), transactionId, timestamp(1)),
            WalRecord.put(
                lsn(1), transactionId, timestamp(10), "key".getBytes(), "value".getBytes()));

    store.recover(records);

    assertEquals("value", store.get("key", timestamp(10)).orElseThrow());
  }

  @Test
  void shouldRecoverDeleteRecord() {
    TransactionId transactionId = TransactionId.generate();

    store.put("key", "value", timestamp(10));

    List<WalRecord> records =
        List.of(WalRecord.delete(lsn(0), transactionId, timestamp(20), "key".getBytes()));

    store.recover(records);

    assertTrue(store.get("key", timestamp(20)).isEmpty());
    assertEquals("value", store.get("key", timestamp(15)).orElseThrow());
  }

  @Test
  void shouldIgnoreBeginRecordDuringRecovery() {
    TransactionId transactionId = TransactionId.generate();

    List<WalRecord> records = List.of(WalRecord.begin(lsn(0), transactionId, timestamp(1)));

    assertDoesNotThrow(() -> store.recover(records));
    assertTrue(store.isEmpty());
  }

  @Test
  void shouldIgnoreCommitAndAbortRecordsDuringRecovery() {
    TransactionId transactionId = TransactionId.generate();

    List<WalRecord> records =
        List.of(
            WalRecord.commit(lsn(0), transactionId, timestamp(10)),
            WalRecord.abort(lsn(1), transactionId, timestamp(20)));

    assertDoesNotThrow(() -> store.recover(records));
    assertTrue(store.isEmpty());
  }

  @Test
  void shouldRejectNullRecoveryRecords() {
    assertThrows(NullPointerException.class, () -> store.recover(null));
  }

  @Test
  void shouldRejectNullRecordDuringRecovery() {
    List<WalRecord> records = new java.util.ArrayList<>();
    records.add(null);

    assertThrows(NullPointerException.class, () -> store.recover(records));
  }

  private LogSequenceNumber lsn(long value) {
    return LogSequenceNumber.of(value);
  }

  private HybridTimestamp timestamp(long physicalTime) {
    return new HybridTimestamp(physicalTime, 0);
  }
}
