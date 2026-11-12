package io.astradb.raft.rpc;

import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.state.RaftNodeState;
import java.util.Objects;

/** Handles incoming Raft RequestVote RPC requests. */
public final class RequestVoteHandler {

  private final RaftNodeState nodeState;
  private final RaftLogStore logStore;

  public RequestVoteHandler(RaftNodeState nodeState, RaftLogStore logStore) {
    this.nodeState = Objects.requireNonNull(nodeState, "Node state must not be null");
    this.logStore = Objects.requireNonNull(logStore, "Log store must not be null");
  }

  /** Evaluates a vote request and returns the voting decision. */
  public RequestVoteResponse handle(RequestVoteRequest request) {
    Objects.requireNonNull(request, "Request must not be null");

    boolean voteGranted =
        nodeState.handleVoteRequest(
            request.getTerm(), request.getCandidateId(), () -> isCandidateLogUpToDate(request));

    return new RequestVoteResponse(nodeState.getCurrentTerm(), voteGranted);
  }

  private boolean isCandidateLogUpToDate(RequestVoteRequest request) {
    long localLastLogTerm = logStore.getLastLogTerm();
    long localLastLogIndex = logStore.getLastLogIndex();

    if (request.getLastLogTerm() != localLastLogTerm) {
      return request.getLastLogTerm() > localLastLogTerm;
    }

    return request.getLastLogIndex() >= localLastLogIndex;
  }
}
