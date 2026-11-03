package io.astradb.storage.mvcc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.common.time.HybridTimestamp;
import org.junit.jupiter.api.Test;

class VersionedValueTest {

  @Test
  void createsRegularValueVersion() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 1L);

    VersionedValue<String> version = VersionedValue.of(timestamp, "hello");

    assertEquals(timestamp, version.getTimestamp());
    assertEquals("hello", version.getValue());
    assertFalse(version.isTombstone());
  }

  @Test
  void createsTombstoneVersion() {
    HybridTimestamp timestamp = new HybridTimestamp(200L, 2L);

    VersionedValue<String> version = VersionedValue.tombstone(timestamp);

    assertEquals(timestamp, version.getTimestamp());
    assertNull(version.getValue());
    assertTrue(version.isTombstone());
  }

  @Test
  void rejectsNullTimestampForRegularValue() {
    assertThrows(NullPointerException.class, () -> VersionedValue.of(null, "hello"));
  }

  @Test
  void rejectsNullTimestampForTombstone() {
    assertThrows(NullPointerException.class, () -> VersionedValue.tombstone(null));
  }

  @Test
  void rejectsNullRegularValue() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 0L);

    assertThrows(NullPointerException.class, () -> VersionedValue.of(timestamp, null));
  }

  @Test
  void regularVersionsWithSameContentAreEqual() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 1L);

    VersionedValue<String> first = VersionedValue.of(timestamp, "hello");
    VersionedValue<String> second = VersionedValue.of(timestamp, "hello");

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  void regularVersionsWithDifferentContentAreNotEqual() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 1L);

    VersionedValue<String> first = VersionedValue.of(timestamp, "hello");
    VersionedValue<String> second = VersionedValue.of(timestamp, "world");

    assertNotEquals(first, second);
  }

  @Test
  void tombstoneAndRegularVersionAreNotEqual() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 1L);

    VersionedValue<String> regular = VersionedValue.of(timestamp, "hello");
    VersionedValue<String> tombstone = VersionedValue.tombstone(timestamp);

    assertNotEquals(regular, tombstone);
  }

  @Test
  void toStringContainsVersionInformation() {
    HybridTimestamp timestamp = new HybridTimestamp(100L, 1L);
    VersionedValue<String> version = VersionedValue.of(timestamp, "hello");

    String result = version.toString();

    assertTrue(result.contains("timestamp=" + timestamp));
    assertTrue(result.contains("value=hello"));
    assertTrue(result.contains("tombstone=false"));
  }
}
