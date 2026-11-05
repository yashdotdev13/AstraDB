package io.astradb.storage.rocksdb;

import java.nio.file.Path;
import java.util.Objects;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Persistent key-value storage backed by RocksDB.
 *
 * <p>Provides basic storage operations for byte-array keys and values.
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

  @Override
  public void close() {
    try {
      database.close();
    } finally {
      options.close();
    }
  }
}
