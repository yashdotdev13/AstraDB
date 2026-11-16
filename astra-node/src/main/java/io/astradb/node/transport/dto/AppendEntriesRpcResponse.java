package io.astradb.node.transport.dto;

import io.astradb.raft.rpc.AppendEntriesResponse;

public record AppendEntriesRpcResponse(long term, boolean success) {

  public static AppendEntriesRpcResponse from(AppendEntriesResponse response) {
    return new AppendEntriesRpcResponse(response.getTerm(), response.isSuccess());
  }

  public AppendEntriesResponse toRaftResponse() {
    return new AppendEntriesResponse(term, success);
  }
}
