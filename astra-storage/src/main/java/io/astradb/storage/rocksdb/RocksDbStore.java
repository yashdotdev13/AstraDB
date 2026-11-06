package io.astradb.storage.rocksdb;

import java.nio.file.Path;
import java.util.Objects;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Persistent key-value storage backed by RocksDB.
 *
 * <p>Provides basic storage operations for byte-array keys and values, including atomic batch
 * writes.
 */
public final class RocksDbStore implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB database;
  private final Options options;

  private RocksDbStore(RocksDB database, Options options) {
    this.database = database;
    this.options = options;
  }

  /**
   * Opens or creates a RocksDB database at the specified path.
   *
   * @param path database directory
   * @return opened RocksDB store
   * @throws RocksDBException if the database cannot be opened
   */
  public static RocksDbStore open(Path path) throws RocksDBException {

    Objects.requireNonNull(path, "Database path must not be null");

    Options options = new Options().setCreateIfMissing(true);

    try {
      RocksDB database = RocksDB.open(options, path.toAbsolutePath().toString());
      return new RocksDbStore(database, options);
    } catch (RocksDBException exception) {
      options.close();
      throw exception;
    }
  }

  /**
   * Stores a key-value pair.
   *
   * @param key key bytes
   * @param value value bytes
   * @throws RocksDBException if the write fails
   */
  public void put(byte[] key, byte[] value) throws RocksDBException {

    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");

    database.put(key, value);
  }

  /**
   * Atomically applies a batch using the default RocksDB durability settings.
   *
   * @param batch prepared RocksDB write batch
   * @throws RocksDBException if the write fails
   */
  public void write(WriteBatch batch) throws RocksDBException {
    write(batch, false);
  }

  /**
   * Atomically applies a batch with configurable synchronous durability.
   *
   * @param batch prepared RocksDB write batch
   * @param sync whether to synchronously persist the WAL
   * @throws RocksDBException if the write fails
   */
  public void write(WriteBatch batch, boolean sync) throws RocksDBException {

    Objects.requireNonNull(batch, "Write batch must not be null");

    try (WriteOptions writeOptions = new WriteOptions().setSync(sync)) {
      database.write(writeOptions, batch);
    }
  }

  /**
   * Retrieves the value associated with a key.
   *
   * @param key key bytes
   * @return value bytes, or null if the key does not exist
   * @throws RocksDBException if the read fails
   */
  public byte[] get(byte[] key) throws RocksDBException {

    Objects.requireNonNull(key, "Key must not be null");

    return database.get(key);
  }

  /**
   * Deletes the entry associated with a key.
   *
   * @param key key bytes
   * @throws RocksDBException if the deletion fails
   */
  public void delete(byte[] key) throws RocksDBException {

    Objects.requireNonNull(key, "Key must not be null");

    database.delete(key);
  }

  /**
   * Flushes pending writes to persistent storage.
   *
   * @throws RocksDBException if the flush fails
   */
  public void flush() throws RocksDBException {

    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      database.flush(flushOptions);
    }
  }

  /**
   * Atomically applies a collection of put and delete operations.
   *
   * @param operations operations to apply
   * @param sync whether to synchronously persist the WAL
   * @throws RocksDBException if the batch write fails
   */
  public void writeBatch(java.util.List<BatchOperation<byte[], byte[]>> operations, boolean sync)
      throws RocksDBException {

    Objects.requireNonNull(operations, "Operations must not be null");

    if (operations.isEmpty()) {
      throw new IllegalArgumentException("Batch must contain at least one operation");
    }

    try (WriteBatch batch = new WriteBatch()) {
      for (BatchOperation<byte[], byte[]> operation : operations) {
        Objects.requireNonNull(operation, "Batch operation must not be null");

        if (operation.getType() == BatchOperation.Type.PUT) {
          batch.put(operation.getKey(), operation.getValue());
        } else {
          batch.delete(operation.getKey());
        }
      }

      write(batch, sync);
    }
  }

  @Override
  public void close() {

    try {
      database.close();
    } finally {
      options.close();
    }
  }
}
