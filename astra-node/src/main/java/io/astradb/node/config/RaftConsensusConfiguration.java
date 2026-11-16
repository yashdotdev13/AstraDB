package io.astradb.node.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.astradb.node.transport.HttpRaftPeerClient;
import io.astradb.node.transport.RaftPeerAddressProvider;
import io.astradb.raft.election.ElectionTimeout;
import io.astradb.raft.election.RaftElectionConfig;
import io.astradb.raft.election.RaftElectionCoordinator;
import io.astradb.raft.election.RaftHeartbeatManager;
import io.astradb.raft.election.RaftPeerClient;
import io.astradb.raft.log.RocksDbRaftLogStore;
import io.astradb.raft.rpc.AppendEntriesHandler;
import io.astradb.raft.rpc.RequestVoteHandler;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftRole;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
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

  /** Configures and validates HTTP addresses for all configured Raft peers. */
  @Bean
  public RaftPeerAddressProvider raftPeerAddressProvider(
      @Value("${astra.raft.peer-addresses:}") String configuredAddresses,
      @Value("${astra.raft.node-id:node-1}") String localNodeId,
      Set<String> raftPeerIds) {

    if (raftPeerIds.contains(localNodeId)) {
      throw new IllegalArgumentException(
          "Local node ID must not be included in the Raft peer list: " + localNodeId);
    }

    RaftPeerAddressProvider provider = new RaftPeerAddressProvider(configuredAddresses);

    for (String peerId : raftPeerIds) {
      provider.getAddress(peerId);
    }

    return provider;
  }

  /** Creates the HTTP client used for inter-node Raft communication. */
  @Bean
  public HttpClient raftHttpClient(
      @Value("${astra.raft.transport.connect-timeout-ms:500}") long connectTimeout) {

    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(connectTimeout))
        .version(HttpClient.Version.HTTP_1_1)
        .build();
  }

  /** Configures the default asynchronous HTTP-based Raft peer client. */
  @Bean
  @ConditionalOnMissingBean(RaftPeerClient.class)
  public RaftPeerClient raftPeerClient(
      HttpClient raftHttpClient,
      ObjectMapper objectMapper,
      RaftPeerAddressProvider addressProvider,
      @Value("${astra.raft.transport.request-timeout-ms:1000}") long requestTimeout) {

    return new HttpRaftPeerClient(
        raftHttpClient, objectMapper, addressProvider, Duration.ofMillis(requestTimeout));
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

  @Bean
  public RequestVoteHandler requestVoteHandler(
      RaftNodeState raftNodeState, RocksDbRaftLogStore raftLogStore) {

    return new RequestVoteHandler(raftNodeState, raftLogStore);
  }

  @Bean
  public AppendEntriesHandler appendEntriesHandler(
      RaftNodeState raftNodeState,
      RocksDbRaftLogStore raftLogStore,
      RaftElectionCoordinator raftElectionCoordinator,
      RaftCommitIndex raftCommitIndex) {

    return new AppendEntriesHandler(
        raftNodeState, raftLogStore, raftElectionCoordinator, raftCommitIndex);
  }
}
