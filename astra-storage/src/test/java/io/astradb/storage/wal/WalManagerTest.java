package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalManagerTest {

  @TempDir Path tempDir;

  private Path walPath;
  private TransactionId transactionId;
  private HybridTimestamp timestamp;

  @BeforeEach
  void setUp() {
    walPath = tempDir.resolve("manager.wal");
    transactionId = TransactionId.generate();
    timestamp = new HybridTimestamp(1000L, 0L);
  }

  @Test
  void shouldAppendTransactionLifecycle() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      LogSequenceNumber begin = manager.begin(transactionId, timestamp);
      LogSequenceNumber put =
          manager.put(transactionId, timestamp, "key".getBytes(), "value".getBytes());
      LogSequenceNumber commit = manager.commit(transactionId, timestamp);

      assertEquals(1L, begin.getValue());
      assertEquals(2L, put.getValue());
      assertEquals(3L, commit.getValue());
    }
  }

  @Test
  void shouldAppendDeleteRecord() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      LogSequenceNumber lsn = manager.delete(transactionId, timestamp, "key".getBytes());

      assertEquals(1L, lsn.getValue());
    }
  }

  @Test
  void shouldAppendAbortRecord() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      LogSequenceNumber lsn = manager.abort(transactionId, timestamp);

      assertEquals(1L, lsn.getValue());
    }
  }

  @Test
  void shouldReadAllRecords() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      manager.begin(transactionId, timestamp);
      manager.put(transactionId, timestamp, "key".getBytes(), "value".getBytes());
      manager.commit(transactionId, timestamp);

      List<WalRecord> records = manager.readAll();

      assertEquals(3, records.size());
    }
  }

  @Test
  void shouldReturnNextLsn() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      assertEquals(1L, manager.getNextLsn().getValue());

      manager.begin(transactionId, timestamp);

      assertEquals(2L, manager.getNextLsn().getValue());
    }
  }

  @Test
  void shouldContinueLsnAfterReopeningWal() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      manager.begin(transactionId, timestamp);
      manager.commit(transactionId, timestamp);
    }

    try (WalManager reopened = new WalManager(walPath)) {
      assertEquals(3L, reopened.getNextLsn().getValue());

      LogSequenceNumber next = reopened.begin(transactionId, timestamp);

      assertEquals(3L, next.getValue());
    }
  }

  @Test
  void shouldFlushSuccessfully() throws IOException {
    try (WalManager manager = new WalManager(walPath)) {
      manager.begin(transactionId, timestamp);

      assertDoesNotThrow(manager::flush);
    }
  }

  @Test
  void shouldRejectOperationsAfterClose() throws IOException {
    WalManager manager = new WalManager(walPath);
    manager.close();

    assertThrows(IOException.class, () -> manager.begin(transactionId, timestamp));
  }

  @Test
  void shouldCloseSuccessfullyMultipleTimes() throws IOException {
    WalManager manager = new WalManager(walPath);

    manager.close();

    assertDoesNotThrow(manager::close);
  }
}
