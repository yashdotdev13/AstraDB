package io.astradb.node.transport;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.astradb.raft.log.RaftLogEntry;
import io.astradb.raft.log.RaftLogStore;
import io.astradb.raft.metadata.RaftMetadata;
import io.astradb.raft.metadata.RaftMetadataStore;
import io.astradb.raft.rpc.AppendEntriesHandler;
import io.astradb.raft.rpc.RequestVoteHandler;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

class RaftRpcControllerTests {

  private MockMvc mockMvc;
  private ObjectMapper objectMapper;
  private InMemoryRaftLogStore logStore;
  private InMemoryRaftMetadataStore metadataStore;
  private RaftNodeState nodeState;
  private int timeoutResetCount;

  @BeforeEach
  void setUp() {
    logStore = new InMemoryRaftLogStore();
    metadataStore = new InMemoryRaftMetadataStore();
    nodeState = new RaftNodeState(metadataStore);
    timeoutResetCount = 0;

    RequestVoteHandler voteHandler = new RequestVoteHandler(nodeState, logStore);

    AppendEntriesHandler appendEntriesHandler =
        new AppendEntriesHandler(
            nodeState, logStore, () -> timeoutResetCount++, new RaftCommitIndex());

    RaftRpcController controller = new RaftRpcController(voteHandler, appendEntriesHandler);

    LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
    validator.afterPropertiesSet();

    mockMvc = MockMvcBuilders.standaloneSetup(controller).setValidator(validator).build();

    objectMapper = new ObjectMapper();
  }

  @Test
  void shouldGrantVoteThroughHttpEndpoint() throws Exception {
    Map<String, Object> request =
        Map.of(
            "term", 1,
            "candidateId", "node-2",
            "lastLogIndex", 0,
            "lastLogTerm", 0);

    mockMvc
        .perform(
            post("/internal/raft/request-vote")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.term").value(1))
        .andExpect(jsonPath("$.voteGranted").value(true));

    org.junit.jupiter.api.Assertions.assertEquals(1, nodeState.getCurrentTerm());
    org.junit.jupiter.api.Assertions.assertEquals(Optional.of("node-2"), nodeState.getVotedFor());
  }

  @Test
  void shouldRejectVoteForAnotherCandidateInSameTerm() throws Exception {
    nodeState.startElection("node-1");

    Map<String, Object> request =
        Map.of(
            "term", 1,
            "candidateId", "node-2",
            "lastLogIndex", 0,
            "lastLogTerm", 0);

    mockMvc
        .perform(
            post("/internal/raft/request-vote")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.term").value(1))
        .andExpect(jsonPath("$.voteGranted").value(false));
  }

  @Test
  void shouldRejectVoteRequestWithBlankCandidateId() throws Exception {
    Map<String, Object> request =
        Map.of(
            "term", 1,
            "candidateId", " ",
            "lastLogIndex", 0,
            "lastLogTerm", 0);

    mockMvc
        .perform(
            post("/internal/raft/request-vote")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isBadRequest());
  }

  @Test
  void shouldAppendEntriesThroughHttpEndpoint() throws Exception {
    Map<String, Object> entry = Map.of("index", 1, "term", 1, "payload", "Y21k");

    Map<String, Object> request =
        Map.of(
            "term", 1,
            "leaderId", "node-1",
            "previousLogIndex", 0,
            "previousLogTerm", 0,
            "entries", List.of(entry),
            "leaderCommit", 1);

    mockMvc
        .perform(
            post("/internal/raft/append-entries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.term").value(1))
        .andExpect(jsonPath("$.success").value(true));

    org.junit.jupiter.api.Assertions.assertEquals(1, logStore.getLastLogIndex());
    org.junit.jupiter.api.Assertions.assertEquals(1, timeoutResetCount);
  }

  @Test
  void shouldRejectAppendEntriesWhenPreviousLogEntryDoesNotMatch() throws Exception {
    Map<String, Object> request =
        Map.of(
            "term", 1,
            "leaderId", "node-1",
            "previousLogIndex", 1,
            "previousLogTerm", 1,
            "entries", List.of(),
            "leaderCommit", 0);

    mockMvc
        .perform(
            post("/internal/raft/append-entries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.term").value(1))
        .andExpect(jsonPath("$.success").value(false));

    org.junit.jupiter.api.Assertions.assertEquals(0, logStore.getLastLogIndex());
    org.junit.jupiter.api.Assertions.assertEquals(1, timeoutResetCount);
  }

  @Test
  void shouldRejectAppendEntriesWithBlankLeaderId() throws Exception {
    Map<String, Object> request =
        Map.of(
            "term", 1,
            "leaderId", " ",
            "previousLogIndex", 0,
            "previousLogTerm", 0,
            "entries", List.of(),
            "leaderCommit", 0);

    mockMvc
        .perform(
            post("/internal/raft/append-entries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isBadRequest());
  }

  private static class InMemoryRaftMetadataStore implements RaftMetadataStore {

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

  private static class InMemoryRaftLogStore implements RaftLogStore {

    private final TreeMap<Long, RaftLogEntry> entries = new TreeMap<>();

    @Override
    public void append(RaftLogEntry entry) {
      entries.put(entry.getIndex(), entry);
    }

    @Override
    public void appendAll(List<RaftLogEntry> newEntries) {
      newEntries.forEach(this::append);
    }

    @Override
    public Optional<RaftLogEntry> getEntry(long index) {
      return Optional.ofNullable(entries.get(index));
    }

    @Override
    public List<RaftLogEntry> getEntries(long fromIndex, long toIndex) {
      return new ArrayList<>(entries.subMap(fromIndex, true, toIndex, true).values());
    }

    @Override
    public long getLastLogIndex() {
      return entries.isEmpty() ? 0 : entries.lastKey();
    }

    @Override
    public long getLastLogTerm() {
      return entries.isEmpty() ? 0 : entries.lastEntry().getValue().getTerm();
    }

    @Override
    public void truncateSuffix(long fromIndex) {
      entries.tailMap(fromIndex, true).clear();
    }

    @Override
    public void flush() {
      // No persistence required for this test implementation.
    }

    @Override
    public void replaceSuffix(long fromIndex, List<RaftLogEntry> newEntries) {
      truncateSuffix(fromIndex);
      appendAll(newEntries);
    }

    @Override
    public void close() {
      entries.clear();
    }
  }
}
