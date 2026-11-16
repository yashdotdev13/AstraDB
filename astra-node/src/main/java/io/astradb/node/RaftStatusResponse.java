package io.astradb.node;

import io.astradb.raft.state.RaftRole;

/** Represents the current consensus status of an AstraDB node. */
public record RaftStatusResponse(
    String nodeId, RaftRole role, long currentTerm, long commitIndex) {}
