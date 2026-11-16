package io.astradb.node.transport.dto;

import io.astradb.raft.rpc.InstallSnapshotResponse;

public record InstallSnapshotRpcResponse(long term, boolean success) {

  public static InstallSnapshotRpcResponse from(InstallSnapshotResponse response) {
    return new InstallSnapshotRpcResponse(response.getTerm(), response.isSuccess());
  }

  public InstallSnapshotResponse toRaftResponse() {
    return new InstallSnapshotResponse(term, success);
  }
}
