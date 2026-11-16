package io.astradb.node.config;

import io.astradb.raft.election.ElectionTimeout;
import io.astradb.raft.election.RaftElectionConfig;
import io.astradb.raft.election.RaftElectionCoordinator;
import io.astradb.raft.election.RaftHeartbeatManager;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.log.RocksDbRaftLogStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configures Raft consensus components and their lifecycle. */
@Configuration
public class RaftConsensusConfiguration {

  @Bean(destroyMethod = "shutdown")
  public ScheduledExecutorService raftScheduler() {
    return Executors.newScheduledThreadPool(2);
  }

  @Bean
  public Set<String> raftPeerIds(@Value("${astra.raft.peers:}") String configuredPeers) {
    if (configuredPeers == null || configuredPeers.isBlank()) {
      return Set.of();
    }

    return Arrays.stream(configuredPeers.split(","))
        .map(String::trim)
        .filter(peer -> !peer.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Bean
  @ConditionalOnMissingBean(RaftPeerClient.class)
  public RaftPeerClient raftPeerClient() {
    return new RaftPeerClient() {

      @Override
      public CompletableFuture<io.astradb.raft.rpc.RequestVoteResponse> requestVote(
          String peerId, io.astradb.raft.rpc.RequestVoteRequest request) {

        return CompletableFuture.failedFuture(
            new IllegalStateException("No Raft network transport configured for peer: " + peerId));
      }

      @Override
      public CompletableFuture<io.astradb.raft.rpc.AppendEntriesResponse> appendEntries(
          String peerId, io.astradb.raft.rpc.AppendEntriesRequest request) {

        return CompletableFuture.failedFuture(
            new IllegalStateException("No Raft network transport configured for peer: " + peerId));
      }
    };
  }

  @Bean
  public ElectionTimeout raftElectionTimeout(
      @Value("${astra.raft.election.minimum-timeout-ms:300}") long minimumTimeout,
      @Value("${astra.raft.election.maximum-timeout-ms:600}") long maximumTimeout) {

    return new ElectionTimeout(
        new RaftElectionConfig(minimumTimeout, maximumTimeout), RandomGenerator.getDefault());
  }

  @Bean(destroyMethod = "close")
  public RaftHeartbeatManager raftHeartbeatManager(
      @Value("${astra.raft.node-id:node-1}") String localNodeId,
      Set<String> raftPeerIds,
      RaftNodeState raftNodeState,
      RocksDbRaftLogStore raftLogStore,
      RaftPeerClient raftPeerClient,
      ScheduledExecutorService raftScheduler,
      RaftCommitIndex raftCommitIndex,
      @Value("${astra.raft.heartbeat-interval-ms:50}") long heartbeatInterval) {

    return new RaftHeartbeatManager(
        localNodeId,
        raftPeerIds,
        raftNodeState,
        raftLogStore,
        raftPeerClient,
        raftScheduler,
        heartbeatInterval,
        raftCommitIndex);
  }

  @Bean(initMethod = "start", destroyMethod = "close")
  public RaftElectionCoordinator raftElectionCoordinator(
      @Value("${astra.raft.node-id:node-1}") String localNodeId,
      Set<String> raftPeerIds,
      RaftNodeState raftNodeState,
      RocksDbRaftLogStore raftLogStore,
      RaftPeerClient raftPeerClient,
      ElectionTimeout raftElectionTimeout,
      ScheduledExecutorService raftScheduler,
      RaftHeartbeatManager raftHeartbeatManager) {

    return new RaftElectionCoordinator(
        localNodeId,
        raftPeerIds,
        raftNodeState,
        raftLogStore,
        raftPeerClient,
        raftElectionTimeout,
        raftScheduler,
        role -> {
          if (role == RaftRole.LEADER) {
            if (!raftPeerIds.isEmpty()) {
              raftHeartbeatManager.start();
            }
          } else {
            raftHeartbeatManager.stop();
          }
        });
  }
}
