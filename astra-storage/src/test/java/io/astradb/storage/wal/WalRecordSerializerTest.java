package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WalRecordSerializerTest {

  private WalRecordSerializer serializer;
  private LogSequenceNumber lsn;
  private TransactionId transactionId;
  private HybridTimestamp timestamp;

  @BeforeEach
  void setUp() {
    serializer = new WalRecordSerializer();
    lsn = LogSequenceNumber.of(42L);
    transactionId = TransactionId.generate();
    timestamp = new HybridTimestamp(1000L, 5);
  }

  @Test
  void shouldSerializeAndDeserializeBeginRecord() throws IOException {
    WalRecord original = WalRecord.begin(lsn, transactionId, timestamp);

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldSerializeAndDeserializePutRecord() throws IOException {
    WalRecord original =
        WalRecord.put(lsn, transactionId, timestamp, "key".getBytes(), "value".getBytes());

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldSerializeAndDeserializeDeleteRecord() throws IOException {
    WalRecord original = WalRecord.delete(lsn, transactionId, timestamp, "key".getBytes());

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldSerializeAndDeserializeCommitRecord() throws IOException {
    WalRecord original = WalRecord.commit(lsn, transactionId, timestamp);

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldSerializeAndDeserializeAbortRecord() throws IOException {
    WalRecord original = WalRecord.abort(lsn, transactionId, timestamp);

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(original, restored);
  }

  @Test
  void shouldPreserveTransactionId() throws IOException {
    WalRecord original = WalRecord.begin(lsn, transactionId, timestamp);

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(transactionId, restored.getTransactionId());
  }

  @Test
  void shouldPreserveHybridTimestamp() throws IOException {
    WalRecord original = WalRecord.begin(lsn, transactionId, timestamp);

    WalRecord restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(timestamp, restored.getTimestamp());
  }

  @Test
  void shouldRejectNullRecord() {
    assertThrows(IllegalArgumentException.class, () -> serializer.serialize(null));
  }

  @Test
  void shouldRejectNullSerializedBytes() {
    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(null));
  }

  @Test
  void shouldRejectTooShortSerializedRecord() {
    assertThrows(IOException.class, () -> serializer.deserialize(new byte[] {1, 2, 3}));
  }

  @Test
  void shouldDetectChecksumMismatch() throws IOException {
    WalRecord original =
        WalRecord.put(lsn, transactionId, timestamp, "key".getBytes(), "value".getBytes());

    byte[] serialized = serializer.serialize(original);
    serialized[10] ^= 0x01;

    assertThrows(IOException.class, () -> serializer.deserialize(serialized));
  }

  @Test
  void shouldRejectInvalidMagicNumber() throws IOException {
    WalRecord original = WalRecord.begin(lsn, transactionId, timestamp);

    byte[] serialized = serializer.serialize(original);
    serialized[0] ^= 0x01;

    // Recalculate checksum so the magic number validation is reached.
    updateChecksum(serialized);

    assertThrows(IOException.class, () -> serializer.deserialize(serialized));
  }

  @Test
  void shouldRejectUnsupportedFormatVersion() throws IOException {
    WalRecord original = WalRecord.begin(lsn, transactionId, timestamp);

    byte[] serialized = serializer.serialize(original);

    // Version is stored at bytes 4 and 5.
    serialized[5] = 2;

    updateChecksum(serialized);

    assertThrows(IOException.class, () -> serializer.deserialize(serialized));
  }

  @Test
  void shouldRejectTruncatedPayload() throws IOException {
    WalRecord original =
        WalRecord.put(lsn, transactionId, timestamp, "key".getBytes(), "value".getBytes());

    byte[] serialized = serializer.serialize(original);
    byte[] truncated = Arrays.copyOf(serialized, serialized.length - 5);

    assertThrows(IOException.class, () -> serializer.deserialize(truncated));
  }

  private void updateChecksum(byte[] serialized) {
    java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
    crc32.update(serialized, 0, serialized.length - Integer.BYTES);

    int checksum = (int) crc32.getValue();
    int offset = serialized.length - Integer.BYTES;

    serialized[offset] = (byte) (checksum >>> 24);
    serialized[offset + 1] = (byte) (checksum >>> 16);
    serialized[offset + 2] = (byte) (checksum >>> 8);
    serialized[offset + 3] = (byte) checksum;
  }
}
