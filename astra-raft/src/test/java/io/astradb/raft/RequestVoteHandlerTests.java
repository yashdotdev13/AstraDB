package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.RequestVoteHandler;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RequestVoteHandlerTests {

  private InMemoryMetadataStore metadataStore;
  private InMemoryLogStore logStore;
  private RaftNodeState nodeState;
  private RequestVoteHandler handler;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryMetadataStore();
    logStore = new InMemoryLogStore();
    nodeState = new RaftNodeState(metadataStore);
    handler = new RequestVoteHandler(nodeState, logStore);
  }

  @Test
  void shouldRejectRequestWithLowerTerm() {
    nodeState.advanceTerm(3);

    RequestVoteResponse response = handler.handle(request(2, "node-2", 0, 0));

    assertFalse(response.isVoteGranted());
    assertEquals(3, response.getTerm());
    assertEquals(3, nodeState.getCurrentTerm());
  }

  @Test
  void shouldAdvanceTermAndClearPreviousVoteWhenReceivingHigherTerm() {
    nodeState.startElection("node-1");

    RequestVoteResponse response = handler.handle(request(3, "node-2", 0, 0));

    assertTrue(response.isVoteGranted());
    assertEquals(3, response.getTerm());
    assertEquals(3, nodeState.getCurrentTerm());
    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertEquals(3, metadataStore.loadMetadata().getCurrentTerm());
  }

  @Test
  void shouldRejectRequestWhenCandidateLogIsOutdated() {
    logStore.setLastLog(5, 3);

    RequestVoteResponse response = handler.handle(request(1, "node-2", 4, 3));

    assertFalse(response.isVoteGranted());
    assertEquals(1, response.getTerm());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldPreferHigherLastLogTermOverHigherLogIndex() {
    logStore.setLastLog(5, 3);

    RequestVoteResponse response = handler.handle(request(1, "node-2", 100, 2));

    assertFalse(response.isVoteGranted());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldGrantVoteWhenCandidateHasHigherLastLogTerm() {
    logStore.setLastLog(10, 3);

    RequestVoteResponse response = handler.handle(request(1, "node-2", 1, 4));

    assertTrue(response.isVoteGranted());
    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldGrantVoteWhenCandidateHasSameLogTermAndIndex() {
    logStore.setLastLog(5, 3);

    RequestVoteResponse response = handler.handle(request(1, "node-2", 5, 3));

    assertTrue(response.isVoteGranted());
    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldGrantRepeatedVoteToSameCandidate() {
    RequestVoteRequest request = request(1, "node-2", 0, 0);

    assertTrue(handler.handle(request).isVoteGranted());
    assertTrue(handler.handle(request).isVoteGranted());
  }

  @Test
  void shouldRejectVoteForDifferentCandidateInSameTerm() {
    assertTrue(handler.handle(request(1, "node-2", 0, 0)).isVoteGranted());

    RequestVoteResponse response = handler.handle(request(1, "node-3", 0, 0));

    assertFalse(response.isVoteGranted());
    assertEquals(1, response.getTerm());
    assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldPersistHigherTermEvenWhenCandidateLogIsOutdated() {
    logStore.setLastLog(5, 3);

    RequestVoteResponse response = handler.handle(request(4, "node-2", 2, 2));

    assertFalse(response.isVoteGranted());
    assertEquals(4, response.getTerm());
    assertEquals(4, metadataStore.loadMetadata().getCurrentTerm());
    assertTrue(metadataStore.loadMetadata().getVotedFor().isEmpty());
  }

  @Test
  void shouldNotGrantVoteWhenPersistenceFails() {
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
    RequestVoteHandler failingHandler = new RequestVoteHandler(state, logStore);

    assertThrows(
        IllegalStateException.class, () -> failingHandler.handle(request(1, "node-2", 0, 0)));

    assertEquals(0, state.getCurrentTerm());
    assertTrue(state.getVotedFor().isEmpty());
    assertEquals(RaftRole.FOLLOWER, state.getRole());
  }

  @Test
  void shouldPreserveCandidateRoleForRepeatedSameTermVote() {
    nodeState.startElection("node-1");

    RequestVoteResponse response = handler.handle(request(1, "node-1", 0, 0));

    assertTrue(response.isVoteGranted());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
  }

  @Test
  void shouldPreserveLeaderRoleForRepeatedSameTermVote() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    RequestVoteResponse response = handler.handle(request(1, "node-1", 0, 0));

    assertTrue(response.isVoteGranted());
    assertEquals(RaftRole.LEADER, nodeState.getRole());
  }

  @Test
  void shouldStepDownWhenReceivingHigherTermEvenIfLogIsOutdated() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();
    logStore.setLastLog(5, 3);

    RequestVoteResponse response = handler.handle(request(2, "node-2", 1, 1));

    assertFalse(response.isVoteGranted());
    assertEquals(2, response.getTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldReturnUpdatedTermWhenHigherTermRequestHasOutdatedLog() {
    nodeState.startElection("node-1");
    logStore.setLastLog(5, 3);

    RequestVoteResponse response = handler.handle(request(4, "node-2", 2, 2));

    assertFalse(response.isVoteGranted());
    assertEquals(4, response.getTerm());
    assertEquals(4, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  private RequestVoteRequest request(
      long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    return new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
  }

  private static final class InMemoryMetadataStore implements RaftMetadataStore {

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

  private static final class InMemoryLogStore implements RaftLogStore {

    private long lastLogIndex;
    private long lastLogTerm;

    void setLastLog(long index, long term) {
      this.lastLogIndex = index;
      this.lastLogTerm = term;
    }

    @Override
    public void append(RaftLogEntry entry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void appendAll(List<RaftLogEntry> entries) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<RaftLogEntry> getEntry(long index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLastLogIndex() {
      return lastLogIndex;
    }

    @Override
    public long getLastLogTerm() {
      return lastLogTerm;
    }

    @Override
    public void truncateSuffix(long fromIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      // No resources to release.
    }
  }
}
