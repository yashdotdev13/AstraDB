package io.astradb.node.config;

import io.astradb.raft.log.RocksDbRaftLogStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configures persistent storage and consensus state for an AstraDB node. */
@Configuration
public class RaftStorageConfiguration {

  @Bean(destroyMethod = "close")
  public RocksDbRaftLogStore raftLogStore(
      @Value("${astra.raft.storage-path:./data/raft}") String storagePath) {

    Path databasePath = Path.of(storagePath).toAbsolutePath().normalize();

    try {
      Files.createDirectories(databasePath);
    } catch (IOException exception) {
      throw new IllegalStateException(
          "Failed to create Raft storage directory: " + databasePath, exception);
    }

    return RocksDbRaftLogStore.open(databasePath);
  }

  @Bean
  public RaftNodeState raftNodeState(RocksDbRaftLogStore raftLogStore) {
    return new RaftNodeState(raftLogStore);
  }

  @Bean
  public RaftCommitIndex raftCommitIndex() {
    return new RaftCommitIndex();
  }
}
