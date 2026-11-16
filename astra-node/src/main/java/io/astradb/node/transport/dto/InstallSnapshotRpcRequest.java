package io.astradb.node.transport.dto;

import io.astradb.raft.rpc.InstallSnapshotRequest;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record InstallSnapshotRpcRequest(
    @Positive long term,
    @NotBlank String leaderId,
    @Positive long lastIncludedIndex,
    @Positive long lastIncludedTerm,
    @NotNull byte[] data) {

  public InstallSnapshotRequest toRaftRequest() {
    return new InstallSnapshotRequest(term, leaderId, lastIncludedIndex, lastIncludedTerm, data);
  }

  public static InstallSnapshotRpcRequest from(InstallSnapshotRequest request) {
    return new InstallSnapshotRpcRequest(
        request.getTerm(),
        request.getLeaderId(),
        request.getLastIncludedIndex(),
        request.getLastIncludedTerm(),
        request.getData());
  }
}
