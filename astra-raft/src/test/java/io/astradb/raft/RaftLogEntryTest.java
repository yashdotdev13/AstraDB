package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogEntry;
import org.junit.jupiter.api.Test;

class RaftLogEntryTest {

  @Test
  void shouldCreateValidLogEntry() {
    byte[] payload = "INSERT INTO users".getBytes();

    RaftLogEntry entry = new RaftLogEntry(1, 1, payload);

    assertEquals(1, entry.getIndex());
    assertEquals(1, entry.getTerm());
    assertArrayEquals(payload, entry.getPayload());
  }

  @Test
  void shouldRejectNonPositiveIndex() {
    assertThrows(IllegalArgumentException.class, () -> new RaftLogEntry(0, 1, new byte[] {1}));
  }

  @Test
  void shouldRejectNonPositiveTerm() {
    assertThrows(IllegalArgumentException.class, () -> new RaftLogEntry(1, 0, new byte[] {1}));
  }

  @Test
  void shouldRejectNullPayload() {
    assertThrows(NullPointerException.class, () -> new RaftLogEntry(1, 1, null));
  }

  @Test
  void shouldDefensivelyCopyPayloadOnConstruction() {
    byte[] payload = {1, 2, 3};

    RaftLogEntry entry = new RaftLogEntry(1, 1, payload);

    payload[0] = 99;

    assertArrayEquals(new byte[] {1, 2, 3}, entry.getPayload());
  }

  @Test
  void shouldReturnDefensiveCopyOfPayload() {
    RaftLogEntry entry = new RaftLogEntry(1, 1, new byte[] {1, 2, 3});

    byte[] returnedPayload = entry.getPayload();
    returnedPayload[0] = 99;

    assertArrayEquals(new byte[] {1, 2, 3}, entry.getPayload());
  }
}
