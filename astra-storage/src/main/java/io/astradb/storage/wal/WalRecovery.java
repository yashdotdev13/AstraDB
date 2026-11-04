package io.astradb.storage.wal;

import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Recovers committed transactions from the write-ahead log.
 *
 * <p>Incomplete and aborted transactions are excluded from recovery.
 */
public final class WalRecovery {

  private final WalRecordSerializer serializer;

  public WalRecovery() {
    this.serializer = new WalRecordSerializer();
  }

  /**
   * Recovers all operations belonging to committed transactions.
   *
   * @param walPath path to the WAL file
   * @return immutable list of recovered records in WAL order
   * @throws IOException if reading or deserializing the WAL fails
   */
  public List<WalRecord> recover(Path walPath) throws IOException {
    Objects.requireNonNull(walPath, "WAL path must not be null");

    List<WalRecord> records;

    try (WalReader reader = new WalReader(walPath, serializer)) {
      records = reader.readAll();
    }

    Map<TransactionId, List<WalRecord>> transactions = new HashMap<>();
    Set<TransactionId> committedTransactions = new HashSet<>();
    Set<TransactionId> abortedTransactions = new HashSet<>();

    for (WalRecord record : records) {
      TransactionId transactionId = record.getTransactionId();

      switch (record.getType()) {
        case BEGIN:
          transactions.computeIfAbsent(transactionId, key -> new ArrayList<>()).add(record);
          break;

        case PUT:
        case DELETE:
          transactions.computeIfAbsent(transactionId, key -> new ArrayList<>()).add(record);
          break;

        case COMMIT:
          committedTransactions.add(transactionId);
          break;

        case ABORT:
          abortedTransactions.add(transactionId);
          break;

        default:
          throw new IllegalStateException("Unexpected WAL record type: " + record.getType());
      }
    }

    List<WalRecord> recoveredRecords = new ArrayList<>();

    for (WalRecord record : records) {
      TransactionId transactionId = record.getTransactionId();

      if (committedTransactions.contains(transactionId)
          && !abortedTransactions.contains(transactionId)
          && record.getType() != WalRecordType.COMMIT
          && record.getType() != WalRecordType.ABORT) {
        recoveredRecords.add(record);
      }
    }

    return Collections.unmodifiableList(recoveredRecords);
  }
}
