package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalReaderTest {

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
  void shouldReadAllWrittenRecords() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false);
      writer.append(
          transactionId, WalRecordType.PUT, timestamp, "key".getBytes(), "value".getBytes(), false);
      writer.append(transactionId, WalRecordType.COMMIT, timestamp, null, null, true);
    }

    try (WalReader reader = new WalReader(walPath, serializer)) {
      List<WalRecord> records = reader.readAll();

      assertEquals(3, records.size());
      assertEquals(WalRecordType.BEGIN, records.get(0).getType());
      assertEquals(WalRecordType.PUT, records.get(1).getType());
      assertEquals(WalRecordType.COMMIT, records.get(2).getType());
    }
  }

  @Test
  void shouldReadRecordsSequentially() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false);
      writer.append(transactionId, WalRecordType.COMMIT, timestamp, null, null, true);
    }

    try (WalReader reader = new WalReader(walPath, serializer)) {
      assertEquals(WalRecordType.BEGIN, reader.readNext().getType());
      assertEquals(WalRecordType.COMMIT, reader.readNext().getType());
      assertNull(reader.readNext());
    }
  }

  @Test
  void shouldReturnEmptyListForEmptyWalFile() throws IOException {
    Files.createFile(walPath);

    try (WalReader reader = new WalReader(walPath, serializer)) {
      assertTrue(reader.readAll().isEmpty());
    }
  }

  @Test
  void shouldRejectNonexistentWalFile() {
    Path nonexistent = tempDir.resolve("missing.wal");

    assertThrows(IOException.class, () -> new WalReader(nonexistent, serializer));
  }

  @Test
  void shouldRejectTruncatedRecordLength() throws IOException {
    Files.write(walPath, new byte[] {0, 0});

    try (WalReader reader = new WalReader(walPath, serializer)) {
      assertThrows(IOException.class, reader::readNext);
    }
  }

  @Test
  void shouldRejectInvalidRecordLength() throws IOException {
    Files.write(walPath, new byte[] {-1, -1, -1, -1});

    try (WalReader reader = new WalReader(walPath, serializer)) {
      assertThrows(IOException.class, reader::readNext);
    }
  }

  @Test
  void shouldRejectTruncatedRecordPayload() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, true);
    }

    byte[] contents = Files.readAllBytes(walPath);
    Files.write(walPath, java.util.Arrays.copyOf(contents, contents.length - 2));

    try (WalReader reader = new WalReader(walPath, serializer)) {
      assertThrows(IOException.class, reader::readNext);
    }
  }

  @Test
  void shouldRejectReadAfterClose() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, true);
    }

    WalReader reader = new WalReader(walPath, serializer);
    reader.close();

    assertThrows(IOException.class, reader::readNext);
  }

  @Test
  void shouldPreserveRecordLsnOrder() throws IOException {
    try (WalWriter writer = new WalWriter(walPath, serializer)) {
      writer.append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false);
      writer.append(transactionId, WalRecordType.COMMIT, timestamp, null, null, true);
    }

    try (WalReader reader = new WalReader(walPath, serializer)) {
      List<WalRecord> records = reader.readAll();

      assertEquals(1L, records.get(0).getLsn().getValue());
      assertEquals(2L, records.get(1).getLsn().getValue());
    }
  }
}
