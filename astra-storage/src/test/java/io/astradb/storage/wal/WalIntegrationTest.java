package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalIntegrationTest {

  @TempDir Path tempDir;

  @Test
  void shouldRecoverCommittedTransactionsAfterWalReopen() throws IOException {
    Path walPath = tempDir.resolve("integration.wal");

    TransactionId committedTransaction = TransactionId.generate();
    TransactionId abortedTransaction = TransactionId.generate();
    TransactionId incompleteTransaction = TransactionId.generate();

    HybridTimestamp timestamp1 = new HybridTimestamp(1000L, 0L);
    HybridTimestamp timestamp2 = new HybridTimestamp(2000L, 0L);
    HybridTimestamp timestamp3 = new HybridTimestamp(3000L, 0L);

    // First WAL lifecycle: write transactions and close.
    try (WalManager manager = new WalManager(walPath)) {
      manager.begin(committedTransaction, timestamp1);
      manager.put(committedTransaction, timestamp1, bytes("user:1"), bytes("Yash"));
      manager.put(committedTransaction, timestamp1, bytes("user:2"), bytes("AstraDB"));
      manager.delete(committedTransaction, timestamp1, bytes("user:3"));
      manager.commit(committedTransaction, timestamp1);

      manager.begin(abortedTransaction, timestamp2);
      manager.put(abortedTransaction, timestamp2, bytes("user:4"), bytes("aborted"));
      manager.abort(abortedTransaction, timestamp2);

      manager.begin(incompleteTransaction, timestamp3);
      manager.put(incompleteTransaction, timestamp3, bytes("user:5"), bytes("incomplete"));
    }

    // Second WAL lifecycle: reopen and recover.
    try (WalManager reopenedManager = new WalManager(walPath)) {
      assertNotNull(reopenedManager.getNextLsn());

      WalRecovery recovery = new WalRecovery();
      List<WalRecord> recovered = recovery.recover(walPath);

      // Only the committed transaction should be recovered.
      assertEquals(4, recovered.size());

      assertEquals(WalRecordType.BEGIN, recovered.get(0).getType());

      assertEquals(WalRecordType.PUT, recovered.get(1).getType());
      assertEquals("user:1", string(recovered.get(1).getKey()));
      assertEquals("Yash", string(recovered.get(1).getValue()));

      assertEquals(WalRecordType.PUT, recovered.get(2).getType());
      assertEquals("user:2", string(recovered.get(2).getKey()));

      assertEquals(WalRecordType.DELETE, recovered.get(3).getType());
      assertEquals("user:3", string(recovered.get(3).getKey()));

      for (WalRecord record : recovered) {
        assertEquals(committedTransaction, record.getTransactionId());
      }
    }
  }

  private byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private String string(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }
}
