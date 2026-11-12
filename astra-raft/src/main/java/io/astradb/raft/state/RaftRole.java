package io.astradb.raft.state;

/** Represents the role of a node in the Raft consensus algorithm. */
public enum RaftRole {
  FOLLOWER,
  CANDIDATE,
  LEADER
}
