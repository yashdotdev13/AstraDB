package io.astradb.raft.rpc;

/** Resets a Raft follower's election timeout after receiving leader communication. */
@FunctionalInterface
public interface ElectionTimeoutResetter {

  void resetElectionTimeout();
}
