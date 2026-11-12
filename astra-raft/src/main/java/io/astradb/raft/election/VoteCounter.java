package io.astradb.raft.election;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Tracks votes received during a Raft election. */
public final class VoteCounter {

  private final int clusterSize;
  private final String localNodeId;
  private final int majority;
  private final Set<String> respondedPeers = new HashSet<>();

  private int grantedVotes = 1;

  public VoteCounter(int clusterSize, String localNodeId) {
    if (clusterSize <= 0) {
      throw new IllegalArgumentException("Cluster size must be positive");
    }

    this.localNodeId = Objects.requireNonNull(localNodeId, "Local node ID must not be null");

    if (localNodeId.isBlank()) {
      throw new IllegalArgumentException("Local node ID must not be blank");
    }

    this.clusterSize = clusterSize;
    this.majority = clusterSize / 2 + 1;
  }

  /**
   * Records a peer's vote.
   *
   * @return true if this peer's response was recorded for the first time
   */
  public synchronized boolean recordVote(String peerId, boolean voteGranted) {
    Objects.requireNonNull(peerId, "Peer ID must not be null");

    if (peerId.isBlank()) {
      throw new IllegalArgumentException("Peer ID must not be blank");
    }

    if (localNodeId.equals(peerId)) {
      throw new IllegalArgumentException("Cannot record a vote from the local node");
    }

    if (respondedPeers.size() >= clusterSize - 1) {
      return false;
    }

    if (!respondedPeers.add(peerId)) {
      return false;
    }

    if (voteGranted) {
      grantedVotes++;
    }

    return true;
  }

  public synchronized int getGrantedVotes() {
    return grantedVotes;
  }

  public int getMajority() {
    return majority;
  }

  public synchronized boolean hasMajority() {
    return grantedVotes >= majority;
  }

  public synchronized boolean isMajorityImpossible() {
    int remainingPeers = clusterSize - 1 - respondedPeers.size();
    return grantedVotes + remainingPeers < majority;
  }
}
