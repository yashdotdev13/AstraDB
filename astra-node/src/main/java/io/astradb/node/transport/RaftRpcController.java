package io.astradb.node.transport;

import io.astradb.node.transport.dto.AppendEntriesRpcRequest;
import io.astradb.node.transport.dto.AppendEntriesRpcResponse;
import io.astradb.node.transport.dto.InstallSnapshotRpcRequest;
import io.astradb.node.transport.dto.InstallSnapshotRpcResponse;
import io.astradb.node.transport.dto.RequestVoteRpcRequest;
import io.astradb.node.transport.dto.RequestVoteRpcResponse;
import io.astradb.raft.rpc.AppendEntriesHandler;
import io.astradb.raft.rpc.InstallSnapshotHandler;
import io.astradb.raft.rpc.RequestVoteHandler;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Exposes inbound Raft consensus RPC endpoints. */
@RestController
@RequestMapping("/internal/raft")
public class RaftRpcController {

  private final RequestVoteHandler requestVoteHandler;
  private final AppendEntriesHandler appendEntriesHandler;
  private final InstallSnapshotHandler installSnapshotHandler;

  public RaftRpcController(
      RequestVoteHandler requestVoteHandler,
      AppendEntriesHandler appendEntriesHandler,
      InstallSnapshotHandler installSnapshotHandler) {

    this.requestVoteHandler = requestVoteHandler;
    this.appendEntriesHandler = appendEntriesHandler;
    this.installSnapshotHandler = installSnapshotHandler;
  }

  /** Processes an incoming RequestVote RPC. */
  @PostMapping("/request-vote")
  public RequestVoteRpcResponse requestVote(@Valid @RequestBody RequestVoteRpcRequest request) {
    return RequestVoteRpcResponse.from(requestVoteHandler.handle(request.toRaftRequest()));
  }

  /** Processes an incoming AppendEntries RPC. */
  @PostMapping("/append-entries")
  public AppendEntriesRpcResponse appendEntries(
      @Valid @RequestBody AppendEntriesRpcRequest request) {
    return AppendEntriesRpcResponse.from(appendEntriesHandler.handle(request.toRaftRequest()));
  }

  /** Processes an incoming InstallSnapshot RPC. */
  @PostMapping("/install-snapshot")
  public InstallSnapshotRpcResponse installSnapshot(
      @Valid @RequestBody InstallSnapshotRpcRequest request) {
    return InstallSnapshotRpcResponse.from(installSnapshotHandler.handle(request.toRaftRequest()));
  }
}
