package io.astradb.raft.rpc;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class InstallSnapshotRequestTests {

  @Test
  void shouldCreateValidRequest() {
    byte[] data = "snapshot-data".getBytes();

    InstallSnapshotRequest request = new InstallSnapshotRequest(5, "node-1", 100, 3, data);

    assertEquals(5, request.getTerm());
    assertEquals("node-1", request.getLeaderId());
    assertEquals(100, request.getLastIncludedIndex());
    assertEquals(3, request.getLastIncludedTerm());
    assertArrayEquals(data, request.getData());
  }

  @Test
  void shouldDefensivelyCopySnapshotData() {
    byte[] data = "snapshot-data".getBytes();

    InstallSnapshotRequest request = new InstallSnapshotRequest(5, "node-1", 100, 3, data);

    data[0] = 0;

    assertArrayEquals("snapshot-data".getBytes(), request.getData());

    byte[] returnedData = request.getData();
    returnedData[0] = 0;

    assertArrayEquals("snapshot-data".getBytes(), request.getData());
  }

  @Test
  void shouldRejectNonPositiveTerm() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InstallSnapshotRequest(0, "node-1", 100, 3, new byte[0]));
  }

  @Test
  void shouldRejectNullLeaderId() {
    assertThrows(
        NullPointerException.class, () -> new InstallSnapshotRequest(5, null, 100, 3, new byte[0]));
  }

  @Test
  void shouldRejectBlankLeaderId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InstallSnapshotRequest(5, " ", 100, 3, new byte[0]));
  }

  @Test
  void shouldRejectNonPositiveLastIncludedIndex() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InstallSnapshotRequest(5, "node-1", 0, 3, new byte[0]));
  }

  @Test
  void shouldRejectNonPositiveLastIncludedTerm() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InstallSnapshotRequest(5, "node-1", 100, 0, new byte[0]));
  }

  @Test
  void shouldRejectLastIncludedTermGreaterThanCurrentTerm() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InstallSnapshotRequest(3, "node-1", 100, 5, new byte[0]));
  }

  @Test
  void shouldRejectNullSnapshotData() {
    assertThrows(
        NullPointerException.class, () -> new InstallSnapshotRequest(5, "node-1", 100, 3, null));
  }

  @Test
  void shouldAllowEmptySnapshotData() {
    InstallSnapshotRequest request = new InstallSnapshotRequest(5, "node-1", 100, 3, new byte[0]);

    assertArrayEquals(new byte[0], request.getData());
  }
}
