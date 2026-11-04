package io.astradb.storage.wal;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.TransactionId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Append-only writer for the write-ahead log.
 *
 * <p>Records are serialized and appended sequentially to a WAL file. The writer supports optional
 * durability guarantees through {@link FileChannel#force(boolean)}.
 */
public final class WalWriter implements AutoCloseable {

  private final Path walPath;
  private final FileChannel channel;
  private final WalRecordSerializer serializer;
  private final ReentrantLock writeLock;

  private long nextLsn;
  private volatile boolean closed;

  /**
   * Creates a WAL writer.
   *
   * @param walPath path to the WAL file
   * @param serializer WAL record serializer
   * @throws IOException if the WAL file cannot be opened
   */
  public WalWriter(Path walPath, WalRecordSerializer serializer) throws IOException {
    this.walPath = Objects.requireNonNull(walPath, "WAL path must not be null");
    this.serializer = Objects.requireNonNull(serializer, "Serializer must not be null");

    Path parent = walPath.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    this.channel =
        FileChannel.open(
            walPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND);

    this.writeLock = new ReentrantLock();
    this.nextLsn = determineNextLsn();
    this.closed = false;
  }

  /**
   * Appends a WAL record and returns its assigned LSN.
   *
   * @param transactionId transaction identifier
   * @param type record type
   * @param timestamp hybrid timestamp
   * @param key record key, if applicable
   * @param value record value, if applicable
   * @param force whether to force the record to stable storage
   * @return assigned log sequence number
   * @throws IOException if writing or forcing the record fails
   */
  public LogSequenceNumber append(
      TransactionId transactionId,
      WalRecordType type,
      HybridTimestamp timestamp,
      byte[] key,
      byte[] value,
      boolean force)
      throws IOException {

    Objects.requireNonNull(transactionId, "Transaction ID must not be null");
    Objects.requireNonNull(type, "Record type must not be null");
    Objects.requireNonNull(timestamp, "Timestamp must not be null");

    writeLock.lock();
    try {
      ensureOpen();

      LogSequenceNumber lsn = LogSequenceNumber.of(nextLsn);

      WalRecord record = createRecord(lsn, transactionId, type, timestamp, key, value);

      byte[] serialized = serializer.serialize(record);
      ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
      lengthBuffer.putInt(serialized.length);
      lengthBuffer.flip();

      while (lengthBuffer.hasRemaining()) {
        channel.write(lengthBuffer);
      }

      ByteBuffer recordBuffer = ByteBuffer.wrap(serialized);

      while (recordBuffer.hasRemaining()) {
        channel.write(recordBuffer);
      }

      if (force) {
        channel.force(true);
      }

      nextLsn++;

      return lsn;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Forces all previously written records to stable storage.
   *
   * @throws IOException if forcing the WAL file fails
   */
  public void flush() throws IOException {
    writeLock.lock();
    try {
      ensureOpen();
      channel.force(true);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the path of the WAL file.
   *
   * @return WAL file path
   */
  public Path getWalPath() {
    return walPath;
  }

  private WalRecord createRecord(
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

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("WAL writer is already closed");
    }
  }

  private long determineNextLsn() throws IOException {
    if (!Files.exists(walPath) || Files.size(walPath) == 0) {
      return 1L;
    }

    try (WalReader reader = new WalReader(walPath, serializer)) {
      long lastLsn = 0L;
      WalRecord record;

      while ((record = reader.readNext()) != null) {
        lastLsn = record.getLsn().getValue();
      }

      return lastLsn + 1;
    }
  }

  public LogSequenceNumber getNextLsn() {
    writeLock.lock();
    try {
      return LogSequenceNumber.of(nextLsn);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Closes the WAL writer.
   *
   * @throws IOException if closing the WAL file fails
   */
  @Override
  public void close() throws IOException {
    writeLock.lock();
    try {
      if (!closed) {
        channel.force(true);
        channel.close();
        closed = true;
      }
    } finally {
      writeLock.unlock();
    }
  }
}
