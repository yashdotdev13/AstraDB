package io.astradb.storage.wal;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Sequential reader for write-ahead log files.
 *
 * <p>Reads length-prefixed serialized WAL records and validates each record using {@link
 * WalRecordSerializer}.
 */
public final class WalReader implements AutoCloseable {

  private static final int MAX_RECORD_SIZE = 128 * 1024 * 1024;

  private final Path walPath;
  private final FileChannel channel;
  private final WalRecordSerializer serializer;

  private volatile boolean closed;

  /**
   * Opens a WAL reader.
   *
   * @param walPath path to the WAL file
   * @param serializer WAL record serializer
   * @throws IOException if the file cannot be opened
   */
  public WalReader(Path walPath, WalRecordSerializer serializer) throws IOException {
    this.walPath = Objects.requireNonNull(walPath, "WAL path must not be null");
    this.serializer = Objects.requireNonNull(serializer, "Serializer must not be null");

    if (!Files.exists(walPath)) {
      throw new IOException("WAL file does not exist: " + walPath);
    }

    this.channel = FileChannel.open(walPath, StandardOpenOption.READ);
    this.closed = false;
  }

  /**
   * Reads the next WAL record.
   *
   * @return next record, or null when the end of the file is reached
   * @throws IOException if the record is truncated or corrupted
   */
  public WalRecord readNext() throws IOException {
    ensureOpen();

    ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);

    int bytesRead = readFullyOrEOF(lengthBuffer);

    if (bytesRead == 0) {
      return null;
    }

    if (bytesRead < Integer.BYTES) {
      throw new EOFException("Incomplete WAL record length at end of file");
    }

    lengthBuffer.flip();
    int recordLength = lengthBuffer.getInt();

    if (recordLength <= 0 || recordLength > MAX_RECORD_SIZE) {
      throw new IOException("Invalid WAL record length: " + recordLength);
    }

    ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);

    int recordBytesRead = readFullyOrEOF(recordBuffer);

    if (recordBytesRead < recordLength) {
      throw new EOFException("Incomplete WAL record at end of file");
    }

    return serializer.deserialize(recordBuffer.array());
  }

  /**
   * Reads all WAL records sequentially.
   *
   * @return immutable list of records
   * @throws IOException if any record is corrupted or truncated
   */
  public List<WalRecord> readAll() throws IOException {
    ensureOpen();

    List<WalRecord> records = new ArrayList<>();
    WalRecord record;

    while ((record = readNext()) != null) {
      records.add(record);
    }

    return Collections.unmodifiableList(records);
  }

  /**
   * Returns the path of the WAL file.
   *
   * @return WAL file path
   */
  public Path getWalPath() {
    return walPath;
  }

  private int readFullyOrEOF(ByteBuffer buffer) throws IOException {
    int totalBytesRead = 0;

    while (buffer.hasRemaining()) {
      int bytesRead = channel.read(buffer);

      if (bytesRead < 0) {
        break;
      }

      if (bytesRead == 0) {
        continue;
      }

      totalBytesRead += bytesRead;
    }

    return totalBytesRead;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("WAL reader is already closed");
    }
  }

  /**
   * Closes the WAL reader.
   *
   * @throws IOException if closing the file fails
   */
  @Override
  public void close() throws IOException {
    if (!closed) {
      channel.close();
      closed = true;
    }
  }
}
