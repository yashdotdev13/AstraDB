package io.astradb.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.raft.election.RaftElectionCoordinator;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftElectionCoordinatorTests {

  private InMemoryMetadataStore metadataStore;
  private RaftNodeState nodeState;
  private InMemoryLogStore logStore;
  private FakePeerClient peerClient;
  private RaftElectionCoordinator coordinator;

  @BeforeEach
  void setUp() {
    metadataStore = new InMemoryMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    logStore = new InMemoryLogStore();
    peerClient = new FakePeerClient();

    coordinator =
        new RaftElectionCoordinator(
            "node-1",
            Set.of("node-2", "node-3", "node-4", "node-5"),
            nodeState,
            logStore,
            peerClient);
  }

  @Test
  void shouldStartElectionAndVoteForItself() {
    long term = coordinator.startElection();

    assertEquals(1, term);
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(Optional.of("node-1"), nodeState.getVotedFor());
    assertEquals(4, peerClient.requests.size());
  }

  @Test
  void shouldBecomeLeaderAfterReceivingMajority() {
    coordinator.startElection();

    peerClient.respond("node-2", 1, true);
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());

    peerClient.respond("node-3", 1, true);

    assertEquals(RaftRole.LEADER, nodeState.getRole());
    assertEquals(1, nodeState.getCurrentTerm());
  }

  @Test
  void shouldNotBecomeLeaderWithoutMajority() {
    coordinator.startElection();

    peerClient.respond("node-2", 1, true);
    peerClient.respond("node-3", 1, false);
    peerClient.respond("node-4", 1, false);
    peerClient.respond("node-5", 1, false);

    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
  }

  @Test
  void shouldBecomeLeaderImmediatelyInSingleNodeCluster() {
    RaftElectionCoordinator singleNodeCoordinator =
        new RaftElectionCoordinator("node-1", Set.of(), nodeState, logStore, peerClient);

    long term = singleNodeCoordinator.startElection();

    assertEquals(1, term);
    assertEquals(RaftRole.LEADER, nodeState.getRole());
    assertTrue(peerClient.requests.isEmpty());
  }

  @Test
  void shouldStepDownWhenPeerReportsHigherTerm() {
    coordinator.startElection();

    peerClient.respond("node-2", 2, false);

    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.FOLLOWER, nodeState.getRole());
    assertTrue(nodeState.getVotedFor().isEmpty());
  }

  @Test
  void shouldIgnoreResponsesFromPreviousElection() {
    coordinator.startElection();

    CompletableFuture<RequestVoteResponse> oldRequest = peerClient.requests.get("node-2");

    coordinator.startElection();

    oldRequest.complete(new RequestVoteResponse(1, true));

    assertEquals(2, nodeState.getCurrentTerm());
    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());

    peerClient.respond("node-3", 2, true);

    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
  }

  @Test
  void shouldCountStaleTermResponseAsRejectedVote() {
    coordinator.startElection();

    peerClient.respond("node-2", 0, true);

    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
    assertEquals(1, nodeState.getCurrentTerm());
  }

  @Test
  void shouldContinueElectionWhenPeerRequestFails() {
    coordinator.startElection();

    peerClient.fail("node-2");
    peerClient.respond("node-3", 1, true);
    peerClient.respond("node-4", 1, true);

    assertEquals(RaftRole.LEADER, nodeState.getRole());
  }

  @Test
  void shouldIgnoreNullOrFailedPeerResponsesWithoutCrashing() {
    coordinator.startElection();

    peerClient.fail("node-2");
    peerClient.respond("node-3", 1, true);
    peerClient.respond("node-4", 1, false);
    peerClient.respond("node-5", 1, false);

    assertEquals(RaftRole.CANDIDATE, nodeState.getRole());
  }

  @Test
  void shouldRejectStartingElectionWhenAlreadyLeader() {
    nodeState.startElection("node-1");
    nodeState.becomeLeader();

    assertThrows(IllegalStateException.class, () -> coordinator.startElection());
  }

  @Test
  void shouldRejectInvalidPeerConfiguration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RaftElectionCoordinator(
                "node-1", Set.of("node-1"), nodeState, logStore, peerClient));

    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftElectionCoordinator("node-1", Set.of(" "), nodeState, logStore, peerClient));
  }

  private static final class FakePeerClient implements RaftPeerClient {

    private final Map<String, CompletableFuture<RequestVoteResponse>> requests = new HashMap<>();

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(
        String peerId, RequestVoteRequest request) {

      CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
      requests.put(peerId, future);
      return future;
    }

    void respond(String peerId, long term, boolean granted) {
      requests.get(peerId).complete(new RequestVoteResponse(term, granted));
    }

    void fail(String peerId) {
      requests.get(peerId).completeExceptionally(new RuntimeException("Peer unavailable"));
    }
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

    @Override
    public void append(RaftLogEntry entry) {}

    @Override
    public void appendAll(List<RaftLogEntry> entries) {}

    @Override
    public Optional<RaftLogEntry> getEntry(long index) {
      return Optional.empty();
    }

    @Override
    public List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      return List.of();
    }

    @Override
    public long getLastLogIndex() {
      return 0;
    }

    @Override
    public long getLastLogTerm() {
      return 0;
    }

    @Override
    public void truncateSuffix(long fromIndex) {}

    @Override
    public void flush() {}

    @Override
    public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {}

    @Override
    public void close() {}
  }
}
