package io.astradb.node.transport.dto;

import io.astradb.raft.rpc.RequestVoteRequest;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

public record RequestVoteRpcRequest(
    @PositiveOrZero long term,
    @NotBlank String candidateId,
    @PositiveOrZero long lastLogIndex,
    @PositiveOrZero long lastLogTerm) {

  public RequestVoteRequest toRaftRequest() {
    return new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
  }

  public static RequestVoteRpcRequest from(RequestVoteRequest request) {
    return new RequestVoteRpcRequest(
        request.getTerm(),
        request.getCandidateId(),
        request.getLastLogIndex(),
        request.getLastLogTerm());
  }
}
