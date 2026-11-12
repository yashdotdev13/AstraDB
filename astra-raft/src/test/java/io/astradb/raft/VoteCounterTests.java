package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.election.VoteCounter;
import org.junit.jupiter.api.Test;

class VoteCounterTests {

  @Test
  void shouldCountLocalVoteInitially() {
    VoteCounter counter = new VoteCounter(5, "node-1");

    assertEquals(1, counter.getGrantedVotes());
    assertEquals(3, counter.getMajority());
    assertFalse(counter.hasMajority());
  }

  @Test
  void shouldWinSingleNodeElectionWithLocalVote() {
    VoteCounter counter = new VoteCounter(1, "node-1");

    assertTrue(counter.hasMajority());
    assertFalse(counter.isMajorityImpossible());
  }

  @Test
  void shouldReachMajorityWhenEnoughPeersGrantVotes() {
    VoteCounter counter = new VoteCounter(5, "node-1");

    assertTrue(counter.recordVote("node-2", true));
    assertTrue(counter.recordVote("node-3", true));

    assertEquals(3, counter.getGrantedVotes());
    assertTrue(counter.hasMajority());
  }

  @Test
  void shouldNotReachMajorityWhenVotesAreRejected() {
    VoteCounter counter = new VoteCounter(5, "node-1");

    counter.recordVote("node-2", false);
    counter.recordVote("node-3", false);

    assertEquals(1, counter.getGrantedVotes());
    assertFalse(counter.hasMajority());
  }

  @Test
  void shouldIgnoreDuplicateVotes() {
    VoteCounter counter = new VoteCounter(3, "node-1");

    assertTrue(counter.recordVote("node-2", true));
    assertFalse(counter.recordVote("node-2", true));

    assertEquals(2, counter.getGrantedVotes());
  }

  @Test
  void shouldNotCountRejectedVoteAsGranted() {
    VoteCounter counter = new VoteCounter(3, "node-1");

    assertTrue(counter.recordVote("node-2", false));

    assertEquals(1, counter.getGrantedVotes());
    assertFalse(counter.hasMajority());
  }

  @Test
  void shouldDetectWhenMajorityBecomesImpossible() {
    VoteCounter counter = new VoteCounter(5, "node-1");

    counter.recordVote("node-2", false);
    counter.recordVote("node-3", false);
    counter.recordVote("node-4", false);

    assertTrue(counter.isMajorityImpossible());
    assertFalse(counter.hasMajority());
  }

  @Test
  void shouldNotDeclareMajorityImpossibleWhileEnoughVotesRemain() {
    VoteCounter counter = new VoteCounter(5, "node-1");

    counter.recordVote("node-2", false);
    counter.recordVote("node-3", false);

    assertFalse(counter.isMajorityImpossible());
  }

  @Test
  void shouldRejectLocalNodeAsPeer() {
    VoteCounter counter = new VoteCounter(3, "node-1");

    assertThrows(IllegalArgumentException.class, () -> counter.recordVote("node-1", true));
  }

  @Test
  void shouldRejectInvalidClusterSize() {
    assertThrows(IllegalArgumentException.class, () -> new VoteCounter(0, "node-1"));
    assertThrows(IllegalArgumentException.class, () -> new VoteCounter(-1, "node-1"));
  }

  @Test
  void shouldRejectInvalidNodeIds() {
    assertThrows(NullPointerException.class, () -> new VoteCounter(3, null));

    assertThrows(IllegalArgumentException.class, () -> new VoteCounter(3, " "));

    VoteCounter counter = new VoteCounter(3, "node-1");

    assertThrows(NullPointerException.class, () -> counter.recordVote(null, true));

    assertThrows(IllegalArgumentException.class, () -> counter.recordVote(" ", true));
  }
}
