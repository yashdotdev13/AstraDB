package io.astradb.raft.election;

import io.astradb.raft.rpc.AppendEntriesRequest;
import io.astradb.raft.rpc.AppendEntriesResponse;
import io.astradb.raft.rpc.RequestVoteRequest;
import io.astradb.raft.rpc.RequestVoteResponse;
import java.util.concurrent.CompletableFuture;

/** Abstraction for sending Raft RPCs to peer nodes. */
public interface RaftPeerClient {

  CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request);

  /**
   * Sends an AppendEntries RPC to a peer asynchronously.
   *
   * @param peerId destination peer identifier
   * @param request append entries request
   * @return future containing the peer's response
   */
  default CompletableFuture<AppendEntriesResponse> appendEntries(
      String peerId, AppendEntriesRequest request) {

    return CompletableFuture.failedFuture(
        new UnsupportedOperationException("AppendEntries RPC is not implemented"));
  }
}
