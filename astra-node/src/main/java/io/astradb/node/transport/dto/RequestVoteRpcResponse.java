package io.astradb.node.transport.dto;

import io.astradb.raft.rpc.RequestVoteResponse;

public record RequestVoteRpcResponse(long term, boolean voteGranted) {

  public static RequestVoteRpcResponse from(RequestVoteResponse response) {
    return new RequestVoteRpcResponse(response.getTerm(), response.isVoteGranted());
  }

  public RequestVoteResponse toRaftResponse() {
    return new RequestVoteResponse(term, voteGranted);
  }
}
