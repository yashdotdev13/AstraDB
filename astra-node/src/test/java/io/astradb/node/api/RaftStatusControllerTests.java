package io.astradb.node.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class RaftStatusControllerTests {

  private MockMvc mockMvc;
  private RaftNodeState nodeState;
  private RaftCommitIndex commitIndex;

  @BeforeEach
  void setUp() {
    RaftMetadataStore metadataStore = new InMemoryMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    commitIndex = new RaftCommitIndex();

    RaftStatusController controller = new RaftStatusController("node-1", nodeState, commitIndex);

    mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
  }

  @Test
  void shouldReturnFollowerStatus() throws Exception {
    mockMvc
        .perform(get("/api/v1/raft/status"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodeId").value("node-1"))
        .andExpect(jsonPath("$.role").value("FOLLOWER"))
        .andExpect(jsonPath("$.currentTerm").value(0))
        .andExpect(jsonPath("$.commitIndex").value(0));
  }

  @Test
  void shouldReturnLeaderStatus() throws Exception {
    nodeState.startElection("node-1");
    nodeState.becomeLeader(1);
    commitIndex.advanceTo(5);

    mockMvc
        .perform(get("/api/v1/raft/status"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodeId").value("node-1"))
        .andExpect(jsonPath("$.role").value("LEADER"))
        .andExpect(jsonPath("$.currentTerm").value(1))
        .andExpect(jsonPath("$.commitIndex").value(5));
  }

  private static final class InMemoryMetadataStore implements RaftMetadataStore {

    private RaftMetadata metadata = new RaftMetadata(0, null);

    @Override
    public RaftMetadata loadMetadata() {
      return metadata;
    }

    @Override
    public void saveMetadata(RaftMetadata metadata) {
      this.metadata = metadata;
    }
  }
}
