package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.raft.election.RaftPeerReplicationState;
import org.junit.jupiter.api.Test;

class RaftPeerReplicationStateTests {

  @Test
  void shouldInitializeReplicationIndexes() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(10);

    assertEquals(10, state.getNextIndex());
    assertEquals(0, state.getMatchIndex());
  }

  @Test
  void shouldAdvanceIndexesAfterSuccessfulReplication() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(10);

    state.recordSuccess(9);

    assertEquals(10, state.getNextIndex());
    assertEquals(9, state.getMatchIndex());
  }

  @Test
  void shouldNotMoveMatchIndexBackwards() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(10);

    state.recordSuccess(9);
    state.recordSuccess(5);

    assertEquals(9, state.getMatchIndex());
    assertEquals(10, state.getNextIndex());
  }

  @Test
  void shouldDecrementNextIndexAfterFailure() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(10);

    state.recordFailure(10);

    assertEquals(9, state.getNextIndex());
    assertEquals(0, state.getMatchIndex());
  }

  @Test
  void shouldNotDecrementNextIndexBelowMatchIndex() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(10);

    state.recordSuccess(9);
    state.recordFailure(10);

    assertEquals(10, state.getNextIndex());
    assertEquals(9, state.getMatchIndex());
  }

  @Test
  void shouldIgnoreStaleFailureResponse() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(10);

    state.recordSuccess(12);
    state.recordFailure(10);

    assertEquals(13, state.getNextIndex());
    assertEquals(12, state.getMatchIndex());
  }

  @Test
  void shouldRejectInvalidInitialNextIndex() {
    assertThrows(IllegalArgumentException.class, () -> new RaftPeerReplicationState(0));
  }

  @Test
  void shouldRejectNegativeReplicatedIndex() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(1);

    assertThrows(IllegalArgumentException.class, () -> state.recordSuccess(-1));
  }

  @Test
  void shouldRejectInvalidAttemptedNextIndex() {
    RaftPeerReplicationState state = new RaftPeerReplicationState(1);

    assertThrows(IllegalArgumentException.class, () -> state.recordFailure(0));
  }
}
