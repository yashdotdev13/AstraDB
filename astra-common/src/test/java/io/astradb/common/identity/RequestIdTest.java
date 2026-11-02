package io.astradb.common.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RequestId}. */
class RequestIdTest {

  @Test
  void shouldGenerateUniqueRequestIds() {
    RequestId first = RequestId.generate();
    RequestId second = RequestId.generate();

    assertNotEquals(first, second);
  }

  @Test
  void shouldCreateRequestIdFromUuid() {
    UUID uuid = UUID.randomUUID();

    RequestId requestId = RequestId.from(uuid);

    assertEquals(uuid, requestId.getValue());
  }

  @Test
  void shouldParseRequestIdFromString() {
    UUID uuid = UUID.randomUUID();

    RequestId requestId = RequestId.parse(uuid.toString());

    assertEquals(uuid, requestId.getValue());
    assertEquals(uuid.toString(), requestId.toString());
  }

  @Test
  void shouldMaintainEqualityAndHashCodeContract() {
    UUID uuid = UUID.randomUUID();

    RequestId first = RequestId.from(uuid);
    RequestId second = RequestId.parse(uuid.toString());

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void shouldRejectInvalidUuidString() {
    assertThrows(IllegalArgumentException.class, () -> RequestId.parse("invalid-request-id"));
  }

  @Test
  void shouldRejectNullUuid() {
    assertThrows(NullPointerException.class, () -> RequestId.from(null));
  }

  @Test
  void shouldRejectNullString() {
    assertThrows(NullPointerException.class, () -> RequestId.parse(null));
  }

  @Test
  void shouldNotEqualObjectsOfDifferentTypes() {
    RequestId requestId = RequestId.generate();

    assertNotEquals(requestId, "some-string");
  }

  @Test
  void shouldHaveUuidStringRepresentation() {
    RequestId requestId = RequestId.generate();

    assertTrue(
        requestId
            .toString()
            .matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
  }
}
