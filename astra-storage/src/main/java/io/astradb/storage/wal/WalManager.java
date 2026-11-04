package io.astradb.storage.wal;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates write-ahead log operations and transaction record persistence.
 *
 * <p>Provides a centralized API for appending transaction records, reading existing records, and
 * managing WAL lifecycle.
 */
public final class WalManager implements AutoCloseable {

  private final Path walPath;
  private final WalRecordSerializer serializer;
  private final WalWriter writer;
  private final ReentrantLock managerLock;

  private long nextLsn;
  private volatile boolean closed;

  /**
   * Creates a WAL manager for the specified file.
   *
   * @param walPath WAL file path
   * @throws IOException if the WAL file cannot be opened
   */
  public WalManager(Path walPath) throws IOException {
    this.walPath = Objects.requireNonNull(walPath, "WAL path must not be null");
    this.serializer = new WalRecordSerializer();

    this.writer = new WalWriter(walPath, serializer);
    this.nextLsn = writer.getNextLsn().getValue();
    this.managerLock = new ReentrantLock();
    this.closed = false;
  }

  /**
   * Appends a BEGIN record for a transaction.
   *
   * @param transactionId transaction identifier
   * @param timestamp hybrid timestamp
   * @return assigned LSN
   * @throws IOException if the record cannot be written
   */
  public LogSequenceNumber begin(TransactionId transactionId, HybridTimestamp timestamp)
      throws IOException {
    return append(transactionId, WalRecordType.BEGIN, timestamp, null, null, false);
  }

  /**
   * Appends a PUT record for a transaction.
   *
   * @param transactionId transaction identifier
   * @param timestamp hybrid timestamp
   * @param key key bytes
   * @param value value bytes
   * @return assigned LSN
   * @throws IOException if the record cannot be written
   */
  public LogSequenceNumber put(
      TransactionId transactionId, HybridTimestamp timestamp, byte[] key, byte[] value)
      throws IOException {
    return append(transactionId, WalRecordType.PUT, timestamp, key, value, false);
  }

  /**
   * Appends a DELETE record for a transaction.
   *
   * @param transactionId transaction identifier
   * @param timestamp hybrid timestamp
   * @param key key bytes
   * @return assigned LSN
   * @throws IOException if the record cannot be written
   */
  public LogSequenceNumber delete(
      TransactionId transactionId, HybridTimestamp timestamp, byte[] key) throws IOException {
    return append(transactionId, WalRecordType.DELETE, timestamp, key, null, false);
  }

  /**
   * Appends a COMMIT record for a transaction and forces it to stable storage.
   *
   * @param transactionId transaction identifier
   * @param timestamp hybrid timestamp
   * @return assigned LSN
   * @throws IOException if the record cannot be written or forced
   */
  public LogSequenceNumber commit(TransactionId transactionId, HybridTimestamp timestamp)
      throws IOException {
    return append(transactionId, WalRecordType.COMMIT, timestamp, null, null, true);
  }

  /**
   * Appends an ABORT record for a transaction.
   *
   * @param transactionId transaction identifier
   * @param timestamp hybrid timestamp
   * @return assigned LSN
   * @throws IOException if the record cannot be written
   */
  public LogSequenceNumber abort(TransactionId transactionId, HybridTimestamp timestamp)
      throws IOException {
    return append(transactionId, WalRecordType.ABORT, timestamp, null, null, true);
  }

  /**
   * Appends a record of the specified type.
   *
   * @param transactionId transaction identifier
   * @param type record type
   * @param timestamp hybrid timestamp
   * @param key optional key
   * @param value optional value
   * @param force whether to force the record to stable storage
   * @return assigned LSN
   * @throws IOException if the record cannot be written
   */
  public LogSequenceNumber append(
      TransactionId transactionId,
      WalRecordType type,
      HybridTimestamp timestamp,
      byte[] key,
      byte[] value,
      boolean force)
      throws IOException {

    managerLock.lock();
    try {
      ensureOpen();

      LogSequenceNumber lsn = writer.append(transactionId, type, timestamp, key, value, force);

      nextLsn = lsn.getValue() + 1;

      return lsn;
    } finally {
      managerLock.unlock();
    }
  }

  /**
   * Reads all records currently present in the WAL.
   *
   * @return immutable list of records
   * @throws IOException if reading fails
   */
  public List<WalRecord> readAll() throws IOException {
    ensureOpen();

    try (WalReader reader = new WalReader(walPath, serializer)) {
      return reader.readAll();
    }
  }

  /**
   * Forces buffered WAL data to stable storage.
   *
   * @throws IOException if forcing the WAL fails
   */
  public void flush() throws IOException {
    managerLock.lock();
    try {
      ensureOpen();
      writer.flush();
    } finally {
      managerLock.unlock();
    }
  }

  /**
   * Returns the next LSN that will be assigned.
   *
   * @return next LSN
   */
  public LogSequenceNumber getNextLsn() {
    managerLock.lock();
    try {
      return LogSequenceNumber.of(nextLsn);
    } finally {
      managerLock.unlock();
    }
  }

  private long readLastLsn(Path path) throws IOException {
    if (!java.nio.file.Files.exists(path) || java.nio.file.Files.size(path) == 0) {
      return 0L;
    }

    try (WalReader reader = new WalReader(path, serializer)) {
      List<WalRecord> records = reader.readAll();

      if (records.isEmpty()) {
        return 0L;
      }

      return records.get(records.size() - 1).getLsn().getValue();
    }
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("WAL manager is already closed");
    }
  }

  /**
   * Closes the WAL manager and its writer.
   *
   * @throws IOException if closing fails
   */
  @Override
  public void close() throws IOException {
    managerLock.lock();
    try {
      if (!closed) {
        writer.close();
        closed = true;
      }
    } finally {
      managerLock.unlock();
    }
  }
}
