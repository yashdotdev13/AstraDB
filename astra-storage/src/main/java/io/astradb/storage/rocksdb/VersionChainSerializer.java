package io.astradb.storage.rocksdb;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.VersionChain;
import io.astradb.storage.mvcc.VersionedValue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;

/**
 * Serializes and deserializes MVCC version chains.
 *
 * @param <V> value type
 */
public final class VersionChainSerializer<V> {

  private static final byte FORMAT_VERSION = 1;
  private static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;
  private static final int VERSION_METADATA_SIZE = (Long.BYTES * 2) + Byte.BYTES + Integer.BYTES;

  private final ValueSerializer<V> valueSerializer;

  public VersionChainSerializer(ValueSerializer<V> valueSerializer) {
    this.valueSerializer =
        Objects.requireNonNull(valueSerializer, "Value serializer must not be null");
  }

  /**
   * Serializes a version chain into bytes.
   *
   * @param chain version chain
   * @return serialized chain
   */
  public byte[] serialize(VersionChain<V> chain) {
    Objects.requireNonNull(chain, "Version chain must not be null");

    List<VersionedValue<V>> versions = chain.getVersions();

    int size = HEADER_SIZE;

    for (VersionedValue<V> version : versions) {
      size += VERSION_METADATA_SIZE;

      if (!version.isTombstone()) {
        byte[] valueBytes = valueSerializer.serialize(version.getValue());
        size += valueBytes.length;
      }
    }

    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);

    buffer.put(FORMAT_VERSION);
    buffer.putInt(versions.size());

    for (VersionedValue<V> version : versions) {
      HybridTimestamp timestamp = version.getTimestamp();

      buffer.putLong(timestamp.getPhysicalTime());
      buffer.putLong(timestamp.getLogicalCounter());
      buffer.put((byte) (version.isTombstone() ? 1 : 0));

      if (version.isTombstone()) {
        buffer.putInt(-1);
      } else {
        byte[] valueBytes = valueSerializer.serialize(version.getValue());

        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }
    }

    return buffer.array();
  }

  /**
   * Deserializes bytes into a version chain.
   *
   * @param bytes serialized chain
   * @return reconstructed version chain
   */
  public VersionChain<V> deserialize(byte[] bytes) {
    Objects.requireNonNull(bytes, "Serialized bytes must not be null");

    if (bytes.length < HEADER_SIZE) {
      throw new IllegalArgumentException("Serialized version chain is too short");
    }

    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

    byte formatVersion = buffer.get();

    if (formatVersion != FORMAT_VERSION) {
      throw new IllegalArgumentException("Unsupported version chain format: " + formatVersion);
    }

    int versionCount = buffer.getInt();

    if (versionCount < 0) {
      throw new IllegalArgumentException("Version count cannot be negative");
    }

    VersionChain<V> chain = new VersionChain<>();

    for (int i = 0; i < versionCount; i++) {
      if (buffer.remaining() < VERSION_METADATA_SIZE) {
        throw new IllegalArgumentException("Incomplete version metadata");
      }

      long physicalTime = buffer.getLong();
      long logicalCounter = buffer.getLong();
      byte tombstoneFlag = buffer.get();

      if (tombstoneFlag != 0 && tombstoneFlag != 1) {
        throw new IllegalArgumentException("Invalid tombstone flag: " + tombstoneFlag);
      }

      int valueLength = buffer.getInt();

      HybridTimestamp timestamp = new HybridTimestamp(physicalTime, logicalCounter);

      if (tombstoneFlag == 1) {
        if (valueLength != -1) {
          throw new IllegalArgumentException("Tombstone must have value length -1");
        }

        chain.add(VersionedValue.tombstone(timestamp));
      } else {
        if (valueLength < 0 || valueLength > buffer.remaining()) {
          throw new IllegalArgumentException("Invalid value length: " + valueLength);
        }

        byte[] valueBytes = new byte[valueLength];
        buffer.get(valueBytes);

        V value = valueSerializer.deserialize(valueBytes);

        chain.add(VersionedValue.of(timestamp, value));
      }
    }

    if (buffer.hasRemaining()) {
      throw new IllegalArgumentException("Unexpected trailing bytes in version chain");
    }

    return chain;
  }
}
