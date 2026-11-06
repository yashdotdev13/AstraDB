package io.astradb.raft.log;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Serializes and deserializes Raft log entries.
 *
 * <p>Binary format:
 *
 * <ul>
 *   <li>Index: 8 bytes
 *   <li>Term: 8 bytes
 *   <li>Payload length: 4 bytes
 *   <li>Payload: variable length
 * </ul>
 */
public final class RaftLogSerializer {

  private static final int HEADER_SIZE = Long.BYTES * 2 + Integer.BYTES;

  /**
   * Serializes a Raft log entry.
   *
   * @param entry entry to serialize
   * @return serialized entry
   */
  public byte[] serialize(RaftLogEntry entry) {
    Objects.requireNonNull(entry, "Raft log entry must not be null");

    byte[] payload = entry.getPayload();

    ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payload.length);

    buffer.putLong(entry.getIndex());
    buffer.putLong(entry.getTerm());
    buffer.putInt(payload.length);
    buffer.put(payload);

    return buffer.array();
  }

  /**
   * Deserializes a Raft log entry.
   *
   * @param data serialized entry
   * @return reconstructed entry
   */
  public RaftLogEntry deserialize(byte[] data) {
    Objects.requireNonNull(data, "Serialized data must not be null");

    if (data.length < HEADER_SIZE) {
      throw new IllegalArgumentException("Serialized Raft log entry is incomplete");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);

    long index = buffer.getLong();
    long term = buffer.getLong();
    int payloadLength = buffer.getInt();

    if (payloadLength < 0 || payloadLength != buffer.remaining()) {
      throw new IllegalArgumentException("Invalid Raft log payload length");
    }

    byte[] payload = new byte[payloadLength];
    buffer.get(payload);

    return new RaftLogEntry(index, term, payload);
  }
}
