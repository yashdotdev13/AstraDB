package io.astradb.storage.wal;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Serializes and deserializes WAL records using a versioned binary format.
 *
 * <p>Each encoded record contains a magic number, format version, record metadata, payloads, and a
 * CRC32 checksum for corruption detection.
 */
public final class WalRecordSerializer {

  private static final int MAGIC_NUMBER = 0x41535452; // ASTB
  private static final short FORMAT_VERSION = 1;

  private static final int CHECKSUM_SIZE = Integer.BYTES;
  private static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;

  /**
   * Serializes a WAL record into its binary representation.
   *
   * @param record record to serialize
   * @return serialized bytes
   * @throws IOException if serialization fails
   */
  public byte[] serialize(WalRecord record) throws IOException {
    if (record == null) {
      throw new IllegalArgumentException("WAL record must not be null");
    }

    ByteArrayOutputStream payloadBuffer = new ByteArrayOutputStream();

    try (DataOutputStream output = new DataOutputStream(payloadBuffer)) {
      output.writeInt(MAGIC_NUMBER);
      output.writeShort(FORMAT_VERSION);

      output.writeLong(record.getLsn().getValue());

      UUID transactionId = record.getTransactionId().getValue();
      output.writeLong(transactionId.getMostSignificantBits());
      output.writeLong(transactionId.getLeastSignificantBits());

      output.writeByte(record.getType().ordinal());

      HybridTimestamp timestamp = record.getTimestamp();
      output.writeLong(timestamp.getPhysicalTime());
      output.writeLong(timestamp.getLogicalCounter());

      writeBytes(output, record.getKey());
      writeBytes(output, record.getValue());
    }

    byte[] payload = payloadBuffer.toByteArray();
    int checksum = calculateChecksum(payload);

    ByteArrayOutputStream resultBuffer = new ByteArrayOutputStream(payload.length + CHECKSUM_SIZE);

    try (DataOutputStream output = new DataOutputStream(resultBuffer)) {
      output.write(payload);
      output.writeInt(checksum);
    }

    return resultBuffer.toByteArray();
  }

  /**
   * Deserializes a WAL record from its binary representation.
   *
   * @param bytes serialized record bytes
   * @return reconstructed WAL record
   * @throws IOException if the record is malformed, corrupted, or unsupported
   */
  public WalRecord deserialize(byte[] bytes) throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Serialized bytes must not be null");
    }

    if (bytes.length < CHECKSUM_SIZE) {
      throw new IOException("Serialized WAL record is too short");
    }

    int payloadLength = bytes.length - CHECKSUM_SIZE;

    byte[] payload = new byte[payloadLength];
    System.arraycopy(bytes, 0, payload, 0, payloadLength);

    int expectedChecksum;
    try (DataInputStream checksumInput =
        new DataInputStream(new ByteArrayInputStream(bytes, payloadLength, CHECKSUM_SIZE))) {
      expectedChecksum = checksumInput.readInt();
    }

    int actualChecksum = calculateChecksum(payload);

    if (expectedChecksum != actualChecksum) {
      throw new IOException("WAL record checksum mismatch");
    }

    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {

      int magicNumber = input.readInt();
      if (magicNumber != MAGIC_NUMBER) {
        throw new IOException("Invalid WAL record magic number");
      }

      short version = input.readShort();
      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported WAL format version: " + version);
      }

      long lsnValue = input.readLong();

      UUID transactionUuid = new UUID(input.readLong(), input.readLong());

      int typeOrdinal = input.readUnsignedByte();
      WalRecordType[] types = WalRecordType.values();

      if (typeOrdinal >= types.length) {
        throw new IOException("Invalid WAL record type: " + typeOrdinal);
      }

      long physicalTime = input.readLong();
      long logicalCounter = input.readLong();

      byte[] key = readBytes(input);
      byte[] value = readBytes(input);

      if (input.available() != 0) {
        throw new IOException("Unexpected trailing bytes in WAL record");
      }

      return reconstructRecord(
          LogSequenceNumber.of(lsnValue),
          TransactionId.from(transactionUuid),
          types[typeOrdinal],
          new HybridTimestamp(physicalTime, logicalCounter),
          key,
          value);
    } catch (IllegalArgumentException | NullPointerException exception) {
      throw new IOException("Invalid WAL record contents", exception);
    }
  }

  private WalRecord reconstructRecord(
      LogSequenceNumber lsn,
      TransactionId transactionId,
      WalRecordType type,
      HybridTimestamp timestamp,
      byte[] key,
      byte[] value) {

    switch (type) {
      case BEGIN:
        return WalRecord.begin(lsn, transactionId, timestamp);

      case PUT:
        return WalRecord.put(lsn, transactionId, timestamp, key, value);

      case DELETE:
        return WalRecord.delete(lsn, transactionId, timestamp, key);

      case COMMIT:
        return WalRecord.commit(lsn, transactionId, timestamp);

      case ABORT:
        return WalRecord.abort(lsn, transactionId, timestamp);

      default:
        throw new IllegalArgumentException("Unsupported WAL record type: " + type);
    }
  }

  private void writeBytes(DataOutputStream output, byte[] bytes) throws IOException {
    if (bytes == null) {
      output.writeInt(-1);
      return;
    }

    if (bytes.length > MAX_PAYLOAD_SIZE) {
      throw new IOException("WAL payload exceeds maximum allowed size");
    }

    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private byte[] readBytes(DataInputStream input) throws IOException {
    int length = input.readInt();

    if (length == -1) {
      return null;
    }

    if (length < 0 || length > MAX_PAYLOAD_SIZE) {
      throw new IOException("Invalid WAL payload length: " + length);
    }

    if (length > input.available()) {
      throw new IOException("Truncated WAL payload");
    }

    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }

  private int calculateChecksum(byte[] bytes) {
    CRC32 crc32 = new CRC32();
    crc32.update(bytes);
    return (int) crc32.getValue();
  }
}
