package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalRecoveryTest {

  @TempDir Path tempDir;

  private Path walPath;
  private WalManager walManager;
  private WalRecovery walRecovery;

  @BeforeEach
  void setUp() throws IOException {
    walPath = tempDir.resolve("test.wal");
    walManager = new WalManager(walPath);
    walRecovery = new WalRecovery();
  }

  @Test
  void shouldRecoverCommittedTransaction() throws IOException {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0L);

    walManager.begin(transactionId, timestamp);
    walManager.put(transactionId, timestamp, bytes("key"), bytes("value"));
    walManager.commit(transactionId, timestamp);

    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(2, recovered.size());
    assertEquals(WalRecordType.BEGIN, recovered.get(0).getType());
    assertEquals(WalRecordType.PUT, recovered.get(1).getType());
    assertEquals("key", string(recovered.get(1).getKey()));
    assertEquals("value", string(recovered.get(1).getValue()));
  }

  @Test
  void shouldIgnoreAbortedTransaction() throws IOException {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0L);

    walManager.begin(transactionId, timestamp);
    walManager.put(transactionId, timestamp, bytes("key"), bytes("value"));
    walManager.abort(transactionId, timestamp);

    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(0, recovered.size());
  }

  @Test
  void shouldIgnoreIncompleteTransaction() throws IOException {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0L);

    walManager.begin(transactionId, timestamp);
    walManager.put(transactionId, timestamp, bytes("key"), bytes("value"));

    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(0, recovered.size());
  }

  @Test
  void shouldRecoverMultipleCommittedTransactionsInOrder() throws IOException {
    TransactionId firstTransaction = TransactionId.generate();
    TransactionId secondTransaction = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(1000L, 0L);

    walManager.begin(firstTransaction, timestamp);
    walManager.put(firstTransaction, timestamp, bytes("first"), bytes("1"));
    walManager.commit(firstTransaction, timestamp);

    walManager.begin(secondTransaction, timestamp);
    walManager.put(secondTransaction, timestamp, bytes("second"), bytes("2"));
    walManager.commit(secondTransaction, timestamp);

    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(4, recovered.size());
    assertEquals(firstTransaction, recovered.get(0).getTransactionId());
    assertEquals(firstTransaction, recovered.get(1).getTransactionId());
    assertEquals(secondTransaction, recovered.get(2).getTransactionId());
    assertEquals(secondTransaction, recovered.get(3).getTransactionId());
  }

  @Test
  void shouldReturnEmptyListForEmptyWal() throws IOException {
    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(0, recovered.size());
  }

  @Test
  void shouldRejectNullWalPath() {
    assertThrows(NullPointerException.class, () -> walRecovery.recover(null));
  }

  @Test
  void shouldRecoverCommittedTransactionAndIgnoreAbortedTransaction() throws IOException {
    TransactionId committed = TransactionId.generate();
    TransactionId aborted = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(2000L, 0L);

    walManager.begin(committed, timestamp);
    walManager.put(committed, timestamp, bytes("committed"), bytes("yes"));
    walManager.commit(committed, timestamp);

    walManager.begin(aborted, timestamp);
    walManager.put(aborted, timestamp, bytes("aborted"), bytes("no"));
    walManager.abort(aborted, timestamp);

    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(2, recovered.size());
    assertEquals(committed, recovered.get(0).getTransactionId());
    assertEquals(committed, recovered.get(1).getTransactionId());
  }

  @Test
  void shouldIgnoreTransactionContainingOnlyBeginRecord() throws IOException {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(3000L, 0L);

    walManager.begin(transactionId, timestamp);
    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertEquals(0, recovered.size());
  }

  @Test
  void shouldReturnImmutableRecoveredRecords() throws IOException {
    TransactionId transactionId = TransactionId.generate();
    HybridTimestamp timestamp = new HybridTimestamp(4000L, 0L);

    walManager.begin(transactionId, timestamp);
    walManager.put(transactionId, timestamp, bytes("key"), bytes("value"));
    walManager.commit(transactionId, timestamp);
    walManager.close();

    List<WalRecord> recovered = walRecovery.recover(walPath);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            recovered.add(
                WalRecord.begin(LogSequenceNumber.of(100), TransactionId.generate(), timestamp)));
  }

  private byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private String string(byte[] value) {
    return new String(value, java.nio.charset.StandardCharsets.UTF_8);
  }
}
