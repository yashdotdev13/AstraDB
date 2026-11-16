package io.astradb.node.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.astradb.node.transport.dto.AppendEntriesRpcRequest;
import io.astradb.node.transport.dto.AppendEntriesRpcResponse;
import io.astradb.node.transport.dto.RequestVoteRpcRequest;
import io.astradb.node.transport.dto.RequestVoteRpcResponse;
import io.astradb.raft.election.ElectionTimeout;
import io.astradb.raft.election.RaftElectionConfig;
import io.astradb.raft.election.RaftElectionCoordinator;
import io.astradb.raft.election.RaftHeartbeatManager;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.AppendEntriesHandler;
import io.astradb.raft.rpc.RequestVoteHandler;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThreeNodeRaftIntegrationTests {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Node node1 = new Node("node-1");
  private final Node node2 = new Node("node-2");
  private final Node node3 = new Node("node-3");

  private RaftHeartbeatManager heartbeatManager;
  private final CountDownLatch leaderElected = new CountDownLatch(1);
  private final CountDownLatch heartbeatsReceived = new CountDownLatch(2);

  @BeforeEach
  void setUp() throws IOException {
    node1.startServer();
    node2.startServer();
    node3.startServer();

    configureNode(node1, Set.of("node-2", "node-3"));
    configureNode(node2, Set.of("node-1", "node-3"));
    configureNode(node3, Set.of("node-1", "node-2"));

    heartbeatManager =
        new RaftHeartbeatManager(
            node1.nodeId,
            Set.of("node-2", "node-3"),
            node1.nodeState,
            node1.logStore,
            node1.peerClient,
            node1.scheduler,
            100,
            node1.commitIndex);
  }

  @AfterEach
  void tearDown() {
    if (heartbeatManager != null) {
      heartbeatManager.close();
    }

    node1.close();
    node2.close();
    node3.close();
  }

  @Test
  void shouldElectLeaderAndSendHeartbeatsAcrossThreeHttpNodes() throws Exception {
    // Start an election on node-1.
    node1.electionCoordinator.startElection();

    // Wait for actual HTTP vote responses from both followers.
    assertTrue(
        leaderElected.await(5, TimeUnit.SECONDS),
        "Node-1 should become leader after receiving majority votes");

    assertEquals(RaftRole.LEADER, node1.nodeState.getRole());
    assertEquals(1, node1.nodeState.getCurrentTerm());

    assertEquals(RaftRole.FOLLOWER, node2.nodeState.getRole());
    assertEquals(RaftRole.FOLLOWER, node3.nodeState.getRole());

    // Start the leader heartbeat manager and send AppendEntries to both followers.
    heartbeatManager.start();
    heartbeatManager.sendHeartbeats();

    assertTrue(
        heartbeatsReceived.await(5, TimeUnit.SECONDS),
        "Both followers should receive HTTP AppendEntries heartbeats");

    assertEquals(RaftRole.FOLLOWER, node2.nodeState.getRole());
    assertEquals(RaftRole.FOLLOWER, node3.nodeState.getRole());

    assertEquals(1, node2.nodeState.getCurrentTerm());
    assertEquals(1, node3.nodeState.getCurrentTerm());
  }

  private void configureNode(Node node, Set<String> peerIds) {
    String addresses =
        peerIds.stream()
            .map(peerId -> peerId + "=" + nodesById().get(peerId).baseAddress())
            .reduce((left, right) -> left + "," + right)
            .orElse("");

    RaftPeerAddressProvider addressProvider = new RaftPeerAddressProvider(addresses);

    RaftPeerClient peerClient =
        new HttpRaftPeerClient(
            HttpClient.newHttpClient(), OBJECT_MAPPER, addressProvider, Duration.ofSeconds(2));

    node.peerClient = peerClient;

    node.electionCoordinator =
        new RaftElectionCoordinator(
            node.nodeId,
            peerIds,
            node.nodeState,
            node.logStore,
            peerClient,
            new ElectionTimeout(
                new RaftElectionConfig(10_000, 20_000),
                java.util.random.RandomGenerator.getDefault()),
            node.scheduler,
            role -> {
              if (role == RaftRole.LEADER && node == node1) {
                leaderElected.countDown();
              }
            });

    node.requestVoteHandler = new RequestVoteHandler(node.nodeState, node.logStore);

    node.appendEntriesHandler =
        new AppendEntriesHandler(
            node.nodeState, node.logStore, node.electionCoordinator, node.commitIndex);

    node.voteHandler.set(node.requestVoteHandler);
    node.appendHandler.set(node.appendEntriesHandler);
  }

  private Map<String, Node> nodesById() {
    return Map.of(
        node1.nodeId, node1,
        node2.nodeId, node2,
        node3.nodeId, node3);
  }

  private final class Node implements AutoCloseable {

    private final String nodeId;
    private final RaftNodeState nodeState = new RaftNodeState(new InMemoryMetadataStore());
    private final InMemoryLogStore logStore = new InMemoryLogStore();
    private final RaftCommitIndex commitIndex = new RaftCommitIndex();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private final AtomicReference<RequestVoteHandler> voteHandler = new AtomicReference<>();
    private final AtomicReference<AppendEntriesHandler> appendHandler = new AtomicReference<>();

    private HttpServer server;
    private RaftPeerClient peerClient;
    private RaftElectionCoordinator electionCoordinator;
    private RequestVoteHandler requestVoteHandler;
    private AppendEntriesHandler appendEntriesHandler;

    private Node(String nodeId) {
      this.nodeId = nodeId;
    }

    private void startServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

      server.createContext("/internal/raft/request-vote", exchange -> handleRequestVote(exchange));

      server.createContext(
          "/internal/raft/append-entries", exchange -> handleAppendEntries(exchange));

      server.start();
    }

    private String baseAddress() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private void handleRequestVote(HttpExchange exchange) throws IOException {
      try (exchange) {
        RequestVoteRpcRequest request =
            OBJECT_MAPPER.readValue(exchange.getRequestBody(), RequestVoteRpcRequest.class);

        RequestVoteHandler handler = voteHandler.get();

        if (handler == null) {
          sendError(exchange);
          return;
        }

        RequestVoteRpcResponse response =
            RequestVoteRpcResponse.from(handler.handle(request.toRaftRequest()));

        sendJson(exchange, response);
      }
    }

    private void handleAppendEntries(HttpExchange exchange) throws IOException {
      try (exchange) {
        AppendEntriesRpcRequest request =
            OBJECT_MAPPER.readValue(exchange.getRequestBody(), AppendEntriesRpcRequest.class);

        AppendEntriesHandler handler = appendHandler.get();

        if (handler == null) {
          sendError(exchange);
          return;
        }

        AppendEntriesRpcResponse response =
            AppendEntriesRpcResponse.from(handler.handle(request.toRaftRequest()));

        sendJson(exchange, response);

        if (nodeId.equals("node-2") || nodeId.equals("node-3")) {
          heartbeatsReceived.countDown();
        }
      }
    }

    private void sendJson(HttpExchange exchange, Object response) throws IOException {
      byte[] body = OBJECT_MAPPER.writeValueAsBytes(response);

      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
    }

    private void sendError(HttpExchange exchange) throws IOException {
      byte[] body = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(500, body.length);
      exchange.getResponseBody().write(body);
    }

    @Override
    public void close() {
      if (electionCoordinator != null) {
        electionCoordinator.close();
      }

      scheduler.shutdownNow();

      if (server != null) {
        server.stop(0);
      }
    }
  }

  private static final class InMemoryMetadataStore implements RaftMetadataStore {

    private RaftMetadata metadata = new RaftMetadata(0, null);

    @Override
    public synchronized RaftMetadata loadMetadata() {
      return metadata;
    }

    @Override
    public synchronized void saveMetadata(RaftMetadata metadata) {
      this.metadata = metadata;
    }
  }

  private static final class InMemoryLogStore implements RaftLogStore {

    private final List<RaftLogEntry> entries = new ArrayList<>();

    @Override
    public synchronized void append(RaftLogEntry entry) {
      appendAll(List.of(entry));
    }

    @Override
    public synchronized void appendAll(List<RaftLogEntry> newEntries) {
      for (RaftLogEntry entry : newEntries) {
        if (entry.getIndex() != entries.size() + 1L) {
          throw new IllegalArgumentException("Non-sequential log entry");
        }

        entries.add(entry);
      }
    }

    @Override
    public synchronized Optional<RaftLogEntry> getEntry(long index) {
      if (index <= 0 || index > entries.size()) {
        return Optional.empty();
      }

      return Optional.of(entries.get((int) index - 1));
    }

    @Override
    public synchronized List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      if (fromIndex <= 0 || toIndex < fromIndex || toIndex > entries.size()) {
        throw new IllegalArgumentException("Invalid log range");
      }

      return List.copyOf(entries.subList((int) fromIndex - 1, (int) toIndex));
    }

    @Override
    public synchronized long getLastLogIndex() {
      return entries.size();
    }

    @Override
    public synchronized long getLastLogTerm() {
      return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).getTerm();
    }

    @Override
    public synchronized void truncateSuffix(long fromIndex) {
      replaceSuffix(fromIndex, List.of());
    }

    @Override
    public synchronized void flush() {}

    @Override
    public synchronized void replaceSuffix(long fromIndex, List<RaftLogEntry> replacements) {
      if (fromIndex <= 0 || fromIndex > entries.size() + 1L) {
        throw new IllegalArgumentException("Invalid replacement index");
      }

      entries.subList((int) fromIndex - 1, entries.size()).clear();
      appendAll(replacements);
    }

    @Override
    public synchronized void close() {}
  }
}
