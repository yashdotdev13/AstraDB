package io.astradb.common.identity;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents the unique identity of an AstraDB cluster node.
 *
 * <p>A NodeId is immutable and suitable for use across distributed operations, cluster membership,
 * and node-to-node communication.
 */
public final class NodeId {

  private final UUID value;

  private NodeId(UUID value) {
    this.value = Objects.requireNonNull(value, "NodeId value must not be null");
  }

  /**
   * Creates a new randomly generated node identity.
   *
   * @return a new unique NodeId
   */
  public static NodeId generate() {
    return new NodeId(UUID.randomUUID());
  }

  /**
   * Creates a NodeId from an existing UUID.
   *
   * @param value UUID representing the node identity
   * @return NodeId instance
   */
  public static NodeId from(UUID value) {
    return new NodeId(value);
  }

  /**
   * Parses a NodeId from its string representation.
   *
   * @param value string representation of a UUID
   * @return parsed NodeId
   * @throws IllegalArgumentException if the value is not a valid UUID
   */
  public static NodeId parse(String value) {
    Objects.requireNonNull(value, "NodeId string must not be null");
    return new NodeId(UUID.fromString(value));
  }

  /**
   * Returns the underlying UUID.
   *
   * @return UUID value
   */
  public UUID getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof NodeId)) {
      return false;
    }
    NodeId other = (NodeId) obj;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
