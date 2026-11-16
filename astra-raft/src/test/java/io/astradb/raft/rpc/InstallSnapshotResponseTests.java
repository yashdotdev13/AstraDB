package io.astradb.raft.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class InstallSnapshotResponseTests {

  @Test
  void shouldCreateSuccessfulResponseWithCurrentTerm() {
    InstallSnapshotResponse response = new InstallSnapshotResponse(5, true);

    assertEquals(5, response.getTerm());
    assertTrue(response.isSuccess());
  }

  @Test
  void shouldCreateRejectedResponse() {
    InstallSnapshotResponse response = new InstallSnapshotResponse(5, false);

    assertEquals(5, response.getTerm());
    assertFalse(response.isSuccess());
  }

  @Test
  void shouldAllowZeroTerm() {
    InstallSnapshotResponse response = new InstallSnapshotResponse(0, true);

    assertEquals(0, response.getTerm());
    assertTrue(response.isSuccess());
  }

  @Test
  void shouldRejectNegativeTerm() {
    assertThrows(IllegalArgumentException.class, () -> new InstallSnapshotResponse(-1, true));
  }
}
