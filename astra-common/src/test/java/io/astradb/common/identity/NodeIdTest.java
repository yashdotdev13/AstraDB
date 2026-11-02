package io.astradb.common.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link NodeId}. */
class NodeIdTest {

  @Test
  void shouldGenerateUniqueNodeIds() {
    NodeId first = NodeId.generate();
    NodeId second = NodeId.generate();

    assertNotEquals(first, second);
  }

  @Test
  void shouldCreateNodeIdFromUuid() {
    UUID uuid = UUID.randomUUID();

    NodeId nodeId = NodeId.from(uuid);

    assertEquals(uuid, nodeId.getValue());
  }

  @Test
  void shouldParseNodeIdFromString() {
    UUID uuid = UUID.randomUUID();

    NodeId nodeId = NodeId.parse(uuid.toString());

    assertEquals(uuid, nodeId.getValue());
    assertEquals(uuid.toString(), nodeId.toString());
  }

  @Test
  void shouldMaintainEqualityAndHashCodeContract() {
    UUID uuid = UUID.randomUUID();

    NodeId first = NodeId.from(uuid);
    NodeId second = NodeId.parse(uuid.toString());

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void shouldRejectInvalidUuidString() {
    assertThrows(IllegalArgumentException.class, () -> NodeId.parse("invalid-node-id"));
  }

  @Test
  void shouldRejectNullUuid() {
    assertThrows(NullPointerException.class, () -> NodeId.from(null));
  }

  @Test
  void shouldRejectNullString() {
    assertThrows(NullPointerException.class, () -> NodeId.parse(null));
  }

  @Test
  void shouldNotEqualObjectsOfDifferentTypes() {
    NodeId nodeId = NodeId.generate();

    assertNotEquals(nodeId, "some-string");
  }

  @Test
  void shouldHaveUuidStringRepresentation() {
    NodeId nodeId = NodeId.generate();

    assertTrue(
        nodeId.toString().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
  }
}
