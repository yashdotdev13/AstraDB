package io.astradb.storage.rocksdb;

import java.util.Arrays;
import java.util.Objects;

/** Immutable byte-array key-value entry read from or written to RocksDB. */
public final class RocksDbEntry {

  private final byte[] key;
  private final byte[] value;

  public RocksDbEntry(byte[] key, byte[] value) {
    Objects.requireNonNull(key, "Key must not be null");
    Objects.requireNonNull(value, "Value must not be null");

    this.key = key.clone();
    this.value = value.clone();
  }

  public byte[] getKey() {
    return key.clone();
  }

  public byte[] getValue() {
    return value.clone();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }

    if (!(object instanceof RocksDbEntry)) {
      return false;
    }

    RocksDbEntry that = (RocksDbEntry) object;
    return Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
  }
}
