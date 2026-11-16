package io.astradb.node.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpRaftPeerClientTests {

  private static final String REQUEST_VOTE_PATH = "/internal/raft/request-vote";
  private static final String APPEND_ENTRIES_PATH = "/internal/raft/append-entries";

  private HttpServer server;
  private HttpRaftPeerClient client;

  private final AtomicReference<String> receivedPath = new AtomicReference<>();
  private final AtomicReference<String> receivedBody = new AtomicReference<>();

  private volatile int responseStatus;
  private volatile String responseBody;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

    responseStatus = 200;
    responseBody = "{}";

    server.createContext(
        "/",
        exchange -> {
          receivedPath.set(exchange.getRequestURI().getPath());
          receivedBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(responseStatus, body.length);

          try (var output = exchange.getResponseBody()) {
            output.write(body);
          }
        });

    server.start();

    String address = "node-2=http://127.0.0.1:" + server.getAddress().getPort();

    client =
        new HttpRaftPeerClient(
            HttpClient.newHttpClient(),
            new ObjectMapper(),
            new RaftPeerAddressProvider(address),
            Duration.ofSeconds(2));
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void shouldSendRequestVoteOverHttp() {
    responseBody =
        """
        {"term": 5, "voteGranted": true}
        """;

    RequestVoteRequest request = new RequestVoteRequest(5, "node-1", 10, 4);

    RequestVoteResponse response = client.requestVote("node-2", request).join();

    assertEquals(5, response.getTerm());
    assertTrue(response.isVoteGranted());
    assertEquals(REQUEST_VOTE_PATH, receivedPath.get());

    assertTrue(receivedBody.get().contains("\"candidateId\":\"node-1\""));
    assertTrue(receivedBody.get().contains("\"lastLogIndex\":10"));
    assertTrue(receivedBody.get().contains("\"lastLogTerm\":4"));
  }

  @Test
  void shouldSendAppendEntriesOverHttp() {
    responseBody =
        """
        {"term": 6, "success": true}
        """;

    AppendEntriesRequest request = new AppendEntriesRequest(6, "node-1", 10, 5, List.of(), 8);

    AppendEntriesResponse response = client.appendEntries("node-2", request).join();

    assertEquals(6, response.getTerm());
    assertTrue(response.isSuccess());
    assertEquals(APPEND_ENTRIES_PATH, receivedPath.get());

    assertTrue(receivedBody.get().contains("\"leaderId\":\"node-1\""));
    assertTrue(receivedBody.get().contains("\"previousLogIndex\":10"));
    assertTrue(receivedBody.get().contains("\"previousLogTerm\":5"));
    assertTrue(receivedBody.get().contains("\"leaderCommit\":8"));
    assertTrue(receivedBody.get().contains("\"entries\":[]"));
  }

  @Test
  void shouldReturnFailedFutureForUnknownPeer() {
    CompletableFuture<RequestVoteResponse> future =
        client.requestVote("unknown-node", new RequestVoteRequest(1, "node-1", 0, 0));

    CompletionException exception = assertThrows(CompletionException.class, future::join);

    assertTrue(hasCause(exception, IllegalArgumentException.class));
  }

  @Test
  void shouldFailWhenPeerReturnsNonSuccessHttpStatus() {
    responseStatus = 503;
    responseBody =
        """
        {"error": "Service unavailable"}
        """;

    CompletableFuture<RequestVoteResponse> future =
        client.requestVote("node-2", new RequestVoteRequest(1, "node-1", 0, 0));

    CompletionException exception = assertThrows(CompletionException.class, future::join);

    assertTrue(hasCause(exception, IllegalStateException.class));
    assertTrue(hasMessage(exception, "HTTP status 503"));
  }

  @Test
  void shouldFailWhenPeerReturnsInvalidJson() {
    responseBody = "not-valid-json";

    CompletableFuture<RequestVoteResponse> future =
        client.requestVote("node-2", new RequestVoteRequest(1, "node-1", 0, 0));

    CompletionException exception = assertThrows(CompletionException.class, future::join);

    assertTrue(hasCause(exception, IllegalStateException.class));
    assertTrue(hasMessage(exception, "Unable to deserialize"));
  }

  private boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
    Throwable current = throwable;

    while (current != null) {
      if (expectedType.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }

    return false;
  }

  private boolean hasMessage(Throwable throwable, String expectedMessage) {
    Throwable current = throwable;

    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expectedMessage)) {
        return true;
      }
      current = current.getCause();
    }

    return false;
  }
}
