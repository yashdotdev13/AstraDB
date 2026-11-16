package io.astradb.node.transport.dto;

import io.astradb.raft.rpc.AppendEntriesRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.List;

public record AppendEntriesRpcRequest(
    @PositiveOrZero long term,
    @NotBlank String leaderId,
    @PositiveOrZero long previousLogIndex,
    @PositiveOrZero long previousLogTerm,
    @NotNull List<@Valid RaftLogEntryRpc> entries,
    @PositiveOrZero long leaderCommit) {

  public AppendEntriesRequest toRaftRequest() {
    return new AppendEntriesRequest(
        term,
        leaderId,
        previousLogIndex,
        previousLogTerm,
        entries.stream().map(RaftLogEntryRpc::toRaftLogEntry).toList(),
        leaderCommit);
  }

  public static AppendEntriesRpcRequest from(AppendEntriesRequest request) {
    return new AppendEntriesRpcRequest(
        request.getTerm(),
        request.getLeaderId(),
        request.getPreviousLogIndex(),
        request.getPreviousLogTerm(),
        request.getEntries().stream().map(RaftLogEntryRpc::from).toList(),
        request.getLeaderCommit());
  }
}
