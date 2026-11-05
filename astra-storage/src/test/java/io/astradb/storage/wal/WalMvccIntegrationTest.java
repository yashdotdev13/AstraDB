package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.MvccStore;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalMvccIntegrationTest {

  @TempDir Path tempDir;

  @Test
  void shouldRecoverCommittedTransactionsAfterRestart() throws IOException {
    Path walPath = tempDir.resolve("integration.wal");

    TransactionId committedTransaction = TransactionId.generate();
    TransactionId abortedTransaction = TransactionId.generate();

    // Write transactions to the WAL.
    try (WalManager walManager = new WalManager(walPath)) {
      walManager.begin(committedTransaction, timestamp(1));

      walManager.put(committedTransaction, timestamp(10), bytes("key"), bytes("value"));

      walManager.commit(committedTransaction, timestamp(20));

      walManager.begin(abortedTransaction, timestamp(21));

      walManager.put(
          abortedTransaction, timestamp(30), bytes("aborted-key"), bytes("aborted-value"));

      walManager.abort(abortedTransaction, timestamp(40));
    }

    // Simulate a restart: create a fresh store and recover from disk.
    MvccStore<String, String> recoveredStore = new MvccStore<>();

    WalRecovery recovery = new WalRecovery();
    List<WalRecord> recoveredRecords = recovery.recover(walPath);

    recoveredStore.recover(recoveredRecords);

    // The committed transaction must be restored.
    assertEquals("value", recoveredStore.get("key", timestamp(20)).orElseThrow());

    // The aborted transaction must not be restored.
    assertTrue(recoveredStore.get("aborted-key", timestamp(40)).isEmpty());

    assertEquals(1, recoveredStore.size());
  }

  @Test
  void shouldRecoverCommittedDeleteAfterRestart() throws IOException {
    Path walPath = tempDir.resolve("delete-integration.wal");

    TransactionId insertTransaction = TransactionId.generate();
    TransactionId deleteTransaction = TransactionId.generate();

    // First commit a value, then commit its deletion.
    try (WalManager walManager = new WalManager(walPath)) {
      walManager.begin(insertTransaction, timestamp(1));

      walManager.put(insertTransaction, timestamp(10), bytes("key"), bytes("value"));

      walManager.commit(insertTransaction, timestamp(15));

      walManager.begin(deleteTransaction, timestamp(16));

      walManager.delete(deleteTransaction, timestamp(20), bytes("key"));

      walManager.commit(deleteTransaction, timestamp(25));
    }

    // Recover into a fresh MVCC store.
    MvccStore<String, String> recoveredStore = new MvccStore<>();

    List<WalRecord> recoveredRecords = new WalRecovery().recover(walPath);

    recoveredStore.recover(recoveredRecords);

    // The value should be visible before deletion.
    assertEquals("value", recoveredStore.get("key", timestamp(15)).orElseThrow());

    // The key should be deleted at the deletion timestamp.
    assertTrue(recoveredStore.get("key", timestamp(25)).isEmpty());
  }

  private byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private HybridTimestamp timestamp(long physicalTime) {
    return new HybridTimestamp(physicalTime, 0);
  }
}
