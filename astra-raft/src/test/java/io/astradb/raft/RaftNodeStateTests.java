package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftNodeStateTests {

  private InMemoryRaftMetadataStore metadataStore;
  private RaftNodeState nodeState;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryRaftMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
  }

  @Test
  void shouldInitializeAsFollowerWithDefaultMetadata() {
    assertEquals(0, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldRestorePersistedMetadataOnRestart() {
    metadataStore.saveMetadata(new RaftMetadata(5, "node-2"));

    RaftNodeState restartedState = new RaftNodeState(metadataStore);

    assertEquals(5, restartedState.getCurrentTerm());
    assertEquals(Optional.of("node-2"), restartedState.getVotedFor());
    assertEquals(RaftRole.FOLLOWER, restartedState.getRole());
  }

  @Test
  void shouldStartElectionAndVoteForSelf() {
    nodeState.startElection("node-1");

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());

    assertEquals(1, metadataStore.loadMetadata().getCurrentTerm());
    assertEquals(Optional.of("node-1"), metadataStore.loadMetadata().getVotedFor());
  }

  @Test
  void shouldIncrementTermWhenStartingAnotherElection() {
    nodeState.startElection("node-1");
    nodeState.startElection("node-1");

    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
  }

  @Test
  void shouldAdvanceToHigherTermAndClearPreviousVote() {
    nodeState.startElection("node-1");

    nodeState.advanceTerm(3);

    assertEquals(3, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
    assertTrue(metadataStore.loadMetadata().getVotedFor().isEmpty());
  }

  @Test
  void shouldRejectAdvancingToCurrentOrLowerTerm() {
    nodeState.startElection("node-1");

    assertThrows(IllegalArgumentException.class, () -> nodeState.advanceTerm(1));
    assertThrows(IllegalArgumentException.class, () -> nodeState.advanceTerm(0));

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
  }

  @Test
  void shouldGrantVoteWhenNoVoteHasBeenCast() {
    assertTrue(nodeState.grantVote("node-2"));

    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
    assertEquals(Optional.of("node-2"), metadataStore.loadMetadata().getVotedFor());
  }

  @Test
  void shouldAllowRepeatedVoteForSameCandidate() {
    assertTrue(nodeState.grantVote("node-2"));
    assertTrue(nodeState.grantVote("node-2"));

    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldRejectVoteForDifferentCandidateInSameTerm() {
    assertTrue(nodeState.grantVote("node-2"));

    assertFalse(nodeState.grantVote("node-3"));

    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldBecomeLeaderOnlyWhenCandidate() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    assertEquals(RaftRole.LEADER, nodeState.getRole());
  }

  @Test
  void shouldRejectBecomingLeaderWhenNotCandidate() {
    assertThrows(IllegalStateException.class, () -> nodeState.becomeLeader());
  }

  @Test
  void shouldBecomeFollowerWithoutChangingTermOrVote() {
    nodeState.startElection("node-1");

    nodeState.becomeFollower();

    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
  }

  @Test
  void shouldNotUpdateInMemoryStateWhenPersistenceFails() {
    RaftMetadataStore failingStore =
        new RaftMetadataStore() {
          @Override
          public RaftMetadata loadMetadata() {
            return new RaftMetadata(0, null);
          }

          @Override
          public void saveMetadata(RaftMetadata metadata) {
            throw new IllegalStateException("Simulated persistence failure");
          }
        };

    RaftNodeState state = new RaftNodeState(failingStore);

    assertThrows(IllegalStateException.class, () -> state.startElection("node-1"));

    assertEquals(0, state.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, state.getRole());
    assertTrue(state.getVotedFor().isEmpty());
  }

  @Test
  void shouldRejectNullAndBlankCandidateIds() {
    assertThrows(NullPointerException.class, () -> nodeState.startElection(null));
    assertThrows(IllegalArgumentException.class, () -> nodeState.startElection(" "));
    assertThrows(NullPointerException.class, () -> nodeState.grantVote(null));
    assertThrows(IllegalArgumentException.class, () -> nodeState.grantVote(""));
  }

  @Test
  void shouldPreserveCandidateRoleWhenReceivingRepeatedSameTermVote() {
    nodeState.startElection("node-1");

    assertTrue(nodeState.grantVote(1, "node-1"));

    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
  }

  @Test
  void shouldPreserveLeaderRoleWhenReceivingRepeatedSameTermVote() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    assertTrue(nodeState.grantVote(1, "node-1"));

    assertEquals(RaftRole.LEADER, nodeState.getRole());
  }

  @Test
  void shouldRejectLowerTermVoteWithoutChangingState() {
    nodeState.startElection("node-1");

    assertFalse(nodeState.grantVote(0, "node-2"));

    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
  }

  @Test
  void shouldStepDownWhenGrantingVoteInHigherTerm() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    assertTrue(nodeState.grantVote(2, "node-2"));

    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldNotEvaluateLogFreshnessForLowerTermRequest() {
    nodeState.startElection("node-1");

    boolean granted =
        nodeState.handleVoteRequest(
            0,
            "node-2",
            () -> {
              fail("Log freshness must not be evaluated for a stale term");
              return false;
            });

    assertFalse(granted);
    assertEquals(1, nodeState.getCurrentTerm());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
  }

  @Test
  void shouldPersistHigherTermEvenWhenLogFreshnessCheckFails() {
    nodeState.startElection("node-1");

    boolean granted = nodeState.handleVoteRequest(3, "node-2", () -> false);

    assertFalse(granted);
    assertEquals(3, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
    assertEquals(3, metadataStore.loadMetadata().getCurrentTerm());
  }

  @Test
  void shouldPersistVoteOnlyAfterLogFreshnessCheckSucceeds() {
    boolean granted = nodeState.handleVoteRequest(1, "node-2", () -> false);

    assertFalse(granted);
    assertTrue(nodeState.getVotedFor().isEmpty());
    assertTrue(metadataStore.loadMetadata().getVotedFor().isEmpty());
  }

  private static final class InMemoryRaftMetadataStore implements RaftMetadataStore {

    private RaftMetadata metadata = new RaftMetadata(0, null);

    @Override
    public RaftMetadata loadMetadata() {
      return metadata;
    }

    @Override
    public void saveMetadata(RaftMetadata metadata) {
      this.metadata = metadata;
    }
  }
}
