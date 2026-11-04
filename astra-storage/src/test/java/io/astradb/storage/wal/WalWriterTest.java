package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalWriterTest {

  @TempDir Path tempDir;

  private Path walPath;
  private WalRecordSerializer serializer;
  private TransactionId transactionId;
  private HybridTimestamp timestamp;

  @BeforeEach
  void setUp() {
    walPath = tempDir.resolve("test.wal");
    serializer = new WalRecordSerializer();
    transactionId = TransactionId.generate();
    timestamp = new HybridTimestamp(1000L, 0L);
  }

  @Test
  void shouldCreateWalFile() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      assertTrue(Files.exists(walPath));
    }
  }

  @Test
  void shouldAppendRecordsSequentially() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      LogSequenceNumber first =
          writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false);

      LogSequenceNumber second =
          writer.append(transactionId, WalRecordType.COMMIT, timestamp, null, null, false);

      assertEquals(1L, first.getValue());
      assertEquals(2L, second.getValue());
    }
  }

  @Test
  void shouldAppendPutRecord() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      LogSequenceNumber lsn =
          writer.append(
              transactionId,
              WalRecordType.PUT,
              timestamp,
              "key".getBytes(),
              "value".getBytes(),
              true);

      assertEquals(1L, lsn.getValue());
      assertTrue(Files.size(walPath) > 0);
    }
  }

  @Test
  void shouldAppendDeleteRecord() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      LogSequenceNumber lsn =
          writer.append(
              transactionId, WalRecordType.DELETE, timestamp, "key".getBytes(), null, true);

      assertEquals(1L, lsn.getValue());
    }
  }

  @Test
  void shouldFlushSuccessfully() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false);

      assertDoesNotThrow(writer::flush);
    }
  }

  @Test
  void shouldRejectAppendAfterClose() throws IOException {
    WalWriter writer = new WalWriter(walPath, serializer);
    writer.close();

    assertThrows(
        IOException.class,
        () -> writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false));
  }

  @Test
  void shouldCloseSuccessfullyMultipleTimes() throws IOException {
    WalWriter writer = new WalWriter(walPath, serializer);

    writer.close();

    assertDoesNotThrow(writer::close);
  }

  @Test
  void shouldRejectNullTransactionId() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      assertThrows(
          NullPointerException.class,
          () -> writer.append(null, WalRecordType.BEGIN, timestamp, null, null, false));
    }
  }

  @Test
  void shouldRejectNullRecordType() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      assertThrows(
          NullPointerException.class,
          () -> writer.append(transactionId, null, timestamp, null, null, false));
    }
  }

  @Test
  void shouldRejectNullTimestamp() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      assertThrows(
          NullPointerException.class,
          () -> writer.append(transactionId, WalRecordType.BEGIN, null, null, null, false));
    }
  }
}
