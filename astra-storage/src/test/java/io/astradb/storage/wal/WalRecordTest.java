package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import org.junit.jupiter.api.Test;

class WalRecordTest {

  private final LogSequenceNumber lsn = LogSequenceNumber.of(1L);
  private final TransactionId transactionId = TransactionId.generate();
  private final HybridTimestamp timestamp = new HybridTimestamp(1000L, 0);

  @Test
  void shouldCreateBeginRecord() {
    WalRecord record = WalRecord.begin(lsn, transactionId, timestamp);

    assertEquals(WalRecordType.BEGIN, record.getType());
    assertNull(record.getKey());
    assertNull(record.getValue());
  }

  @Test
  void shouldCreatePutRecord() {
    byte[] key = "key".getBytes();
    byte[] value = "value".getBytes();

    WalRecord record = WalRecord.put(lsn, transactionId, timestamp, key, value);

    assertEquals(WalRecordType.PUT, record.getType());
    assertArrayEquals(key, record.getKey());
    assertArrayEquals(value, record.getValue());
  }

  @Test
  void shouldCreateDeleteRecord() {
    byte[] key = "key".getBytes();

    WalRecord record = WalRecord.delete(lsn, transactionId, timestamp, key);

    assertEquals(WalRecordType.DELETE, record.getType());
    assertArrayEquals(key, record.getKey());
    assertNull(record.getValue());
  }

  @Test
  void shouldCreateCommitRecord() {
    WalRecord record = WalRecord.commit(lsn, transactionId, timestamp);

    assertEquals(WalRecordType.COMMIT, record.getType());
    assertNull(record.getKey());
    assertNull(record.getValue());
  }

  @Test
  void shouldCreateAbortRecord() {
    WalRecord record = WalRecord.abort(lsn, transactionId, timestamp);

    assertEquals(WalRecordType.ABORT, record.getType());
    assertNull(record.getKey());
    assertNull(record.getValue());
  }

  @Test
  void shouldRejectNullRequiredFields() {
    assertThrows(NullPointerException.class, () -> WalRecord.begin(null, transactionId, timestamp));

    assertThrows(NullPointerException.class, () -> WalRecord.begin(lsn, null, timestamp));

    assertThrows(NullPointerException.class, () -> WalRecord.begin(lsn, transactionId, null));
  }

  @Test
  void shouldRejectEmptyPutKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WalRecord.put(lsn, transactionId, timestamp, new byte[0], new byte[] {1}));
  }

  @Test
  void shouldRejectEmptyPutValue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WalRecord.put(lsn, transactionId, timestamp, new byte[] {1}, new byte[0]));
  }

  @Test
  void shouldRejectNullPutPayload() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WalRecord.put(lsn, transactionId, timestamp, null, new byte[] {1}));

    assertThrows(
        IllegalArgumentException.class,
        () -> WalRecord.put(lsn, transactionId, timestamp, new byte[] {1}, null));
  }

  @Test
  void shouldRejectEmptyDeleteKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WalRecord.delete(lsn, transactionId, timestamp, new byte[0]));
  }

  @Test
  void shouldRejectDeleteWithValue() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WalRecord(
                lsn,
                transactionId,
                WalRecordType.DELETE,
                timestamp,
                new byte[] {1},
                new byte[] {2}));
  }

  @Test
  void shouldDefensivelyCopyKeyAndValue() {
    byte[] key = {1, 2};
    byte[] value = {3, 4};

    WalRecord record = WalRecord.put(lsn, transactionId, timestamp, key, value);

    key[0] = 9;
    value[0] = 9;

    assertArrayEquals(new byte[] {1, 2}, record.getKey());
    assertArrayEquals(new byte[] {3, 4}, record.getValue());
  }

  @Test
  void shouldDefensivelyCopyReturnedPayloads() {
    WalRecord record =
        WalRecord.put(lsn, transactionId, timestamp, new byte[] {1, 2}, new byte[] {3, 4});

    byte[] returnedKey = record.getKey();
    byte[] returnedValue = record.getValue();

    returnedKey[0] = 9;
    returnedValue[0] = 9;

    assertArrayEquals(new byte[] {1, 2}, record.getKey());
    assertArrayEquals(new byte[] {3, 4}, record.getValue());
  }

  @Test
  void shouldImplementEqualityAndHashCode() {
    byte[] key = {1};
    byte[] value = {2};

    WalRecord first = WalRecord.put(lsn, transactionId, timestamp, key, value);
    WalRecord second = WalRecord.put(lsn, transactionId, timestamp, key, value);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void shouldImplementToString() {
    WalRecord record = WalRecord.begin(lsn, transactionId, timestamp);

    assertTrue(record.toString().contains("WalRecord"));
    assertTrue(record.toString().contains("BEGIN"));
  }
}
