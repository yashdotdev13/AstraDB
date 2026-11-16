package io.astradb.node.api;

import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Exposes the current Raft consensus status of this node. */
@RestController
@RequestMapping("/api/v1/raft")
public class RaftStatusController {

  private final String nodeId;
  private final RaftNodeState nodeState;
  private final RaftCommitIndex commitIndex;

  public RaftStatusController(
      @Value("${astra.raft.node-id:node-1}") String nodeId,
      RaftNodeState nodeState,
      RaftCommitIndex commitIndex) {

    this.nodeId = nodeId;
    this.nodeState = nodeState;
    this.commitIndex = commitIndex;
  }

  /** Returns the current node identity, role, term, and commit index. */
  @GetMapping("/status")
  public RaftStatusResponse getStatus() {
    return new RaftStatusResponse(
        nodeId, nodeState.getRole(), nodeState.getCurrentTerm(), commitIndex.getCommitIndex());
  }
}
