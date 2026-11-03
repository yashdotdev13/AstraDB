package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class TransactionIdTest {

  @Test
  void generateCreatesUniqueTransactionIds() {
    TransactionId first = TransactionId.generate();
    TransactionId second = TransactionId.generate();

    assertNotNull(first);
    assertNotNull(second);
    assertNotEquals(first, second);
  }

  @Test
  void fromCreatesTransactionIdFromUuid() {
    UUID uuid = UUID.randomUUID();

    TransactionId transactionId = TransactionId.from(uuid);

    assertEquals(uuid, transactionId.getValue());
  }

  @Test
  void fromRejectsNullUuid() {
    assertThrows(NullPointerException.class, () -> TransactionId.from(null));
  }

  @Test
  void parseCreatesTransactionIdFromString() {
    UUID uuid = UUID.randomUUID();

    TransactionId transactionId = TransactionId.parse(uuid.toString());

    assertEquals(uuid, transactionId.getValue());
  }

  @Test
  void parseRejectsNullString() {
    assertThrows(NullPointerException.class, () -> TransactionId.parse(null));
  }

  @Test
  void parseRejectsInvalidUuidString() {
    assertThrows(IllegalArgumentException.class, () -> TransactionId.parse("invalid-uuid"));
  }

  @Test
  void transactionIdsWithSameUuidAreEqual() {
    UUID uuid = UUID.randomUUID();

    TransactionId first = TransactionId.from(uuid);
    TransactionId second = TransactionId.from(uuid);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void transactionIdsWithDifferentUuidsAreNotEqual() {
    TransactionId first = TransactionId.generate();
    TransactionId second = TransactionId.generate();

    assertNotEquals(first, second);
  }

  @Test
  void transactionIdIsNotEqualToNullOrDifferentType() {
    TransactionId transactionId = TransactionId.generate();

    assertNotEquals(null, transactionId);
    assertNotEquals(transactionId, UUID.randomUUID());
  }

  @Test
  void toStringReturnsUuidRepresentation() {
    UUID uuid = UUID.randomUUID();
    TransactionId transactionId = TransactionId.from(uuid);

    assertEquals(uuid.toString(), transactionId.toString());
  }
}
