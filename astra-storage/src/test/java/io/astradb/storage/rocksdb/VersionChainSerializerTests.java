package io.astradb.storage.rocksdb;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.common.time.HybridTimestamp;
import io.astradb.storage.mvcc.VersionChain;
import io.astradb.storage.mvcc.VersionedValue;
import org.junit.jupiter.api.Test;

class VersionChainSerializerTests {

  private final VersionChainSerializer<String> serializer =
      new VersionChainSerializer<>(new StringValueSerializer());

  @Test
  void shouldSerializeAndDeserializeSingleVersion() {
    VersionChain<String> chain = new VersionChain<>();
    chain.add(VersionedValue.of(new HybridTimestamp(100, 1), "value"));

    VersionChain<String> restored = serializer.deserialize(serializer.serialize(chain));

    assertEquals(chain.getVersions(), restored.getVersions());
  }

  @Test
  void shouldPreserveMultipleVersionsInDescendingOrder() {
    VersionChain<String> chain = new VersionChain<>();
    chain.add(VersionedValue.of(new HybridTimestamp(100, 1), "older"));
    chain.add(VersionedValue.of(new HybridTimestamp(200, 1), "newer"));

    VersionChain<String> restored = serializer.deserialize(serializer.serialize(chain));

    assertEquals(2, restored.size());
    assertEquals(new HybridTimestamp(200, 1), restored.getVersions().get(0).getTimestamp());
    assertEquals("newer", restored.getVersions().get(0).getValue());
    assertEquals("older", restored.getVersions().get(1).getValue());
  }

  @Test
  void shouldPreserveTombstone() {
    VersionChain<String> chain = new VersionChain<>();
    chain.add(VersionedValue.of(new HybridTimestamp(100, 1), "value"));
    chain.add(VersionedValue.tombstone(new HybridTimestamp(200, 1)));

    VersionChain<String> restored = serializer.deserialize(serializer.serialize(chain));

    assertEquals(2, restored.size());
    assertTrue(restored.getVersions().get(0).isTombstone());
    assertEquals(new HybridTimestamp(200, 1), restored.getVersions().get(0).getTimestamp());
  }

  @Test
  void shouldSerializeAndDeserializeEmptyChain() {
    VersionChain<String> chain = new VersionChain<>();

    VersionChain<String> restored = serializer.deserialize(serializer.serialize(chain));

    assertTrue(restored.isEmpty());
  }

  @Test
  void shouldPreserveUnicodeValues() {
    VersionChain<String> chain = new VersionChain<>();
    String value = "AstraDB 数据库 ";
    chain.add(VersionedValue.of(new HybridTimestamp(100, 1), value));

    VersionChain<String> restored = serializer.deserialize(serializer.serialize(chain));

    assertEquals(value, restored.getVersions().get(0).getValue());
  }

  @Test
  void shouldRejectNullChain() {
    assertThrows(NullPointerException.class, () -> serializer.serialize(null));
  }

  @Test
  void shouldRejectNullBytes() {
    assertThrows(NullPointerException.class, () -> serializer.deserialize(null));
  }

  @Test
  void shouldRejectUnsupportedFormatVersion() {
    byte[] bytes = serializer.serialize(new VersionChain<>());
    bytes[0] = 99;

    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(bytes));
  }

  @Test
  void shouldRejectTruncatedData() {
    byte[] bytes = serializer.serialize(new VersionChain<>());
    byte[] truncated = new byte[] {bytes[0]};

    assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(truncated));
  }
}
