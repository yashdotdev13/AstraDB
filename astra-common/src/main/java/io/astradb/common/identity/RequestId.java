package io.astradb.common.identity;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents a unique identifier for a request within AstraDB.
 *
 * <p>A RequestId is immutable and can be propagated across nodes for distributed tracing, request
 * correlation, and diagnostic purposes.
 */
public final class RequestId {

  private final UUID value;

  private RequestId(UUID value) {
    this.value = Objects.requireNonNull(value, "RequestId value must not be null");
  }

  public static RequestId generate() {
    return new RequestId(UUID.randomUUID());
  }

  public static RequestId from(UUID value) {
    return new RequestId(value);
  }

  public static RequestId parse(String value) {
    Objects.requireNonNull(value, "RequestId string must not be null");
    return new RequestId(UUID.fromString(value));
  }

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
    if (!(obj instanceof RequestId)) {
      return false;
    }
    RequestId other = (RequestId) obj;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
