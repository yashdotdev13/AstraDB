package io.astradb.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WalRecordTypeTest {

  @Test
  void shouldDefineAllSupportedRecordTypes() {
    assertArrayEquals(
        new WalRecordType[] {
          WalRecordType.BEGIN,
          WalRecordType.PUT,
          WalRecordType.DELETE,
          WalRecordType.COMMIT,
          WalRecordType.ABORT
        },
        WalRecordType.values());
  }

  @Test
  void shouldResolveRecordTypeByName() {
    assertEquals(WalRecordType.BEGIN, WalRecordType.valueOf("BEGIN"));
    assertEquals(WalRecordType.PUT, WalRecordType.valueOf("PUT"));
    assertEquals(WalRecordType.DELETE, WalRecordType.valueOf("DELETE"));
    assertEquals(WalRecordType.COMMIT, WalRecordType.valueOf("COMMIT"));
    assertEquals(WalRecordType.ABORT, WalRecordType.valueOf("ABORT"));
  }

  @Test
  void shouldRejectUnknownRecordType() {
    assertThrows(IllegalArgumentException.class, () -> WalRecordType.valueOf("CHECKPOINT"));
  }
}
