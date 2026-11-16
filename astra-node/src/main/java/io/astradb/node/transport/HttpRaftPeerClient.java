package io.astradb.node.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.astradb.node.transport.dto.AppendEntriesRpcRequest;
import io.astradb.node.transport.dto.AppendEntriesRpcResponse;
import io.astradb.node.transport.dto.RequestVoteRpcRequest;
import io.astradb.node.transport.dto.RequestVoteRpcResponse;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Sends Raft RPC requests to remote nodes over HTTP. */
public final class HttpRaftPeerClient implements RaftPeerClient {

  private static final String REQUEST_VOTE_PATH = "/internal/raft/request-vote";
  private static final String APPEND_ENTRIES_PATH = "/internal/raft/append-entries";

  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final RaftPeerAddressProvider addressProvider;
  private final Duration requestTimeout;

  public HttpRaftPeerClient(
      HttpClient httpClient,
      ObjectMapper objectMapper,
      RaftPeerAddressProvider addressProvider,
      Duration requestTimeout) {

    this.httpClient = Objects.requireNonNull(httpClient);
    this.objectMapper = Objects.requireNonNull(objectMapper);
    this.addressProvider = Objects.requireNonNull(addressProvider);
    this.requestTimeout = Objects.requireNonNull(requestTimeout);

    if (requestTimeout.isZero() || requestTimeout.isNegative()) {
      throw new IllegalArgumentException("Request timeout must be positive");
    }
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(
      String peerId, RequestVoteRequest request) {

    return post(
            peerId,
            REQUEST_VOTE_PATH,
            RequestVoteRpcRequest.from(request),
            RequestVoteRpcResponse.class)
        .thenApply(RequestVoteRpcResponse::toRaftResponse);
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(
      String peerId, AppendEntriesRequest request) {

    return post(
            peerId,
            APPEND_ENTRIES_PATH,
            AppendEntriesRpcRequest.from(request),
            AppendEntriesRpcResponse.class)
        .thenApply(AppendEntriesRpcResponse::toRaftResponse);
  }

  private <T> CompletableFuture<T> post(
      String peerId, String path, Object payload, Class<T> responseType) {

    try {
      URI baseAddress = addressProvider.getAddress(peerId);
      URI endpoint = URI.create(baseAddress.toString().replaceAll("/+$", "") + path);

      String requestBody = objectMapper.writeValueAsString(payload);

      HttpRequest httpRequest =
          HttpRequest.newBuilder()
              .uri(endpoint)
              .timeout(requestTimeout)
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(requestBody))
              .build();

      return httpClient
          .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
          .thenApply(
              response -> {
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                  throw new CompletionException(
                      new IllegalStateException(
                          "Raft RPC to peer "
                              + peerId
                              + " failed with HTTP status "
                              + response.statusCode()));
                }

                try {
                  return objectMapper.readValue(response.body(), responseType);
                } catch (IOException exception) {
                  throw new CompletionException(
                      new IllegalStateException(
                          "Unable to deserialize Raft RPC response from peer: " + peerId,
                          exception));
                }
              });
    } catch (Exception exception) {
      return CompletableFuture.failedFuture(exception);
    }
  }
}
