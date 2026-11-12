package io.astradb.raft.election;

import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import java.util.concurrent.CompletableFuture;

/** Abstraction for sending Raft RequestVote RPCs to peer nodes. */
public interface RaftPeerClient {

  /**
   * Sends a RequestVote RPC to a peer asynchronously.
   *
   * @param peerId identifier of the destination peer
   * @param request the vote request
   * @return future containing the peer's response
   */
  CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request);
}
