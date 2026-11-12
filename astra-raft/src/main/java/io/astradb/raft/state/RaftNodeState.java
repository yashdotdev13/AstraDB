package io.astradb.raft.state;

import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/** Manages the persistent term and voting state of a Raft node. */
public final class RaftNodeState {

  private final RaftMetadataStore metadataStore;

  private long currentTerm;
  private String votedFor;
  private RaftRole role;

  public RaftNodeState(RaftMetadataStore metadataStore) {
    this.metadataStore = Objects.requireNonNull(metadataStore, "Metadata store must not be null");

    RaftMetadata metadata = metadataStore.loadMetadata();

    this.currentTerm = metadata.getCurrentTerm();
    this.votedFor = metadata.getVotedFor().orElse(null);
    this.role = RaftRole.FOLLOWER;
  }

  public synchronized long getCurrentTerm() {
    return currentTerm;
  }

  public synchronized RaftRole getRole() {
    return role;
  }

  public synchronized Optional<String> getVotedFor() {
    return Optional.ofNullable(votedFor);
  }

  /** Starts an election by incrementing the term and voting for this node. */
  public synchronized void startElection(String candidateId) {
    Objects.requireNonNull(candidateId, "Candidate ID must not be null");

    if (candidateId.isBlank()) {
      throw new IllegalArgumentException("Candidate ID must not be blank");
    }

    long newTerm = Math.incrementExact(currentTerm);

    metadataStore.saveMetadata(new RaftMetadata(newTerm, candidateId));

    currentTerm = newTerm;
    votedFor = candidateId;
    role = RaftRole.CANDIDATE;
  }

  /** Advances to a higher term and becomes a follower. */
  public synchronized void advanceTerm(long newTerm) {
    if (newTerm <= currentTerm) {
      throw new IllegalArgumentException("New term must be greater than the current term");
    }

    metadataStore.saveMetadata(new RaftMetadata(newTerm, null));

    currentTerm = newTerm;
    votedFor = null;
    role = RaftRole.FOLLOWER;
  }

  /**
   * Processes a vote request atomically with respect to other node state transitions.
   *
   * @param requestTerm the candidate's term
   * @param candidateId the candidate requesting the vote
   * @param isCandidateLogUpToDate checks whether the candidate's log is sufficiently up to date
   * @return true if the vote is granted
   */
  public synchronized boolean handleVoteRequest(
      long requestTerm, String candidateId, BooleanSupplier isCandidateLogUpToDate) {

    Objects.requireNonNull(candidateId, "Candidate ID must not be null");
    Objects.requireNonNull(isCandidateLogUpToDate, "Log freshness checker must not be null");

    if (candidateId.isBlank()) {
      throw new IllegalArgumentException("Candidate ID must not be blank");
    }

    if (requestTerm < currentTerm) {
      return false;
    }

    // Persist a higher term and step down before evaluating the candidate's log.
    if (requestTerm > currentTerm) {
      metadataStore.saveMetadata(new RaftMetadata(requestTerm, null));

      currentTerm = requestTerm;
      votedFor = null;
      role = RaftRole.FOLLOWER;
    }

    if (!isCandidateLogUpToDate.getAsBoolean()) {
      return false;
    }

    if (votedFor != null && !votedFor.equals(candidateId)) {
      return false;
    }

    // Persist the vote before granting it.
    if (votedFor == null) {
      metadataStore.saveMetadata(new RaftMetadata(currentTerm, candidateId));
      votedFor = candidateId;
    }

    return true;
  }

  /** Processes a vote request in the current term without a log freshness check. */
  public synchronized boolean grantVote(long requestTerm, String candidateId) {
    return handleVoteRequest(requestTerm, candidateId, () -> true);
  }

  /** Processes a vote request in the current term. */
  public synchronized boolean grantVote(String candidateId) {
    return grantVote(currentTerm, candidateId);
  }

  /** Transitions this node to leader after winning an election. */
  public synchronized void becomeLeader() {
    if (role != RaftRole.CANDIDATE) {
      throw new IllegalStateException("Only a candidate can become a leader");
    }

    role = RaftRole.LEADER;
  }

  /** Steps down to follower without changing the current term or vote. */
  public synchronized void becomeFollower() {
    role = RaftRole.FOLLOWER;
  }
}
