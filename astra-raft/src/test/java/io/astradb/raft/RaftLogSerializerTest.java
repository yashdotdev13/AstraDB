package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogSerializer;
import org.junit.jupiter.api.Test;

class RaftLogSerializerTest {

  private final RaftLogSerializer serializer = new RaftLogSerializer();

  @Test
  void shouldSerializeAndDeserializeLogEntry() {
    RaftLogEntry original = new RaftLogEntry(10, 3, "INSERT INTO users".getBytes());

    byte[] serialized = serializer.serialize(original);

    RaftLogEntry restored = serializer.deserialize(serialized);

    assertEquals(original.getIndex(), restored.getIndex());
    assertEquals(original.getTerm(), restored.getTerm());
    assertArrayEquals(original.getPayload(), restored.getPayload());
  }

  @Test
  void shouldSupportEmptyPayload() {
    RaftLogEntry original = new RaftLogEntry(1, 1, new byte[0]);

    RaftLogEntry restored = serializer.deserialize(serializer.serialize(original));

    assertEquals(1, restored.getIndex());
    assertEquals(1, restored.getTerm());
    assertArrayEquals(new byte[0], restored.getPayload());
  }

  @Test
  void shouldRejectNullEntry() {
    assertThrows(NullPointerException.class, () -> serializer.serialize(null));
  }

  @Test
  void shouldRejectNullSerializedData() {
    assertThrows(NullPointerException.class, () -> serializer.deserialize(null));
  }

  @Test
  void shouldRejectIncompleteHeader() {
    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(new byte[10]));
  }

  @Test
  void shouldRejectNegativePayloadLength() {
    byte[] data = java.nio.ByteBuffer.allocate(20).putLong(1).putLong(1).putInt(-1).array();

    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(data));
  }

  @Test
  void shouldRejectTruncatedPayload() {
    byte[] data =
        java.nio.ByteBuffer.allocate(21).putLong(1).putLong(1).putInt(5).put((byte) 1).array();

    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(data));
  }

  @Test
  void shouldRejectTrailingBytes() {
    byte[] data =
        java.nio.ByteBuffer.allocate(21).putLong(1).putLong(1).putInt(0).put((byte) 1).array();

    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(data));
  }

  @Test
  void shouldRejectInvalidLogIndex() {
    byte[] data = java.nio.ByteBuffer.allocate(20).putLong(0).putLong(1).putInt(0).array();

    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(data));
  }
}
