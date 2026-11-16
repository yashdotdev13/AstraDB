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
import io.astradb.raft.rpc.InstallSnapshotHandler;
import io.astradb.raft.rpc.RequestVoteHandler;
import io.astradb.raft.snapshot.InMemoryRaftSnapshotStore;
import io.astradb.raft.state.RaftCommitIndex;
import io.astradb.raft.state.RaftNodeState;
import io.astradb.raft.state.RaftStateMachine;
import io.astradb.raft.state.RaftStateMachineApplier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
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
  private InMemoryRaftSnapshotStore snapshotStore;
  private RaftNodeState nodeState;
  private RaftCommitIndex commitIndex;
  private RaftStateMachineApplier stateMachineApplier;
  private byte[] restoredSnapshotData;
  private int timeoutResetCount;

  @BeforeEach
  void setUp() {
    logStore = new InMemoryRaftLogStore();
    metadataStore = new InMemoryRaftMetadataStore();
    snapshotStore = new InMemoryRaftSnapshotStore();
    nodeState = new RaftNodeState(metadataStore);
    commitIndex = new RaftCommitIndex();
    timeoutResetCount = 0;
    restoredSnapshotData = null;

    RaftStateMachine stateMachine =
        new RaftStateMachine() {
          @Override
          public void apply(long index, byte[] command) {
            // No commands are applied in this controller test.
          }

          @Override
          public void restore(byte[] snapshotData) {
            restoredSnapshotData = snapshotData.clone();
          }
        };

    stateMachineApplier = new RaftStateMachineApplier(logStore, commitIndex, stateMachine);

    RequestVoteHandler voteHandler = new RequestVoteHandler(nodeState, logStore);

    AppendEntriesHandler appendEntriesHandler =
        new AppendEntriesHandler(nodeState, logStore, () -> timeoutResetCount++, commitIndex);

    InstallSnapshotHandler installSnapshotHandler =
        new InstallSnapshotHandler(
            nodeState, logStore, snapshotStore, () -> timeoutResetCount++, stateMachineApplier);

    RaftRpcController controller =
        new RaftRpcController(voteHandler, appendEntriesHandler, installSnapshotHandler);

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

  @Test
  void shouldInstallSnapshotThroughHttpEndpoint() throws Exception {
    byte[] snapshotData = "snapshot-data".getBytes(StandardCharsets.UTF_8);

    Map<String, Object> request =
        Map.of(
            "term", 2,
            "leaderId", "node-2",
            "lastIncludedIndex", 5,
            "lastIncludedTerm", 2,
            "data", Base64.getEncoder().encodeToString(snapshotData));

    mockMvc
        .perform(
            post("/internal/raft/install-snapshot")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.term").value(2))
        .andExpect(jsonPath("$.success").value(true));

    org.junit.jupiter.api.Assertions.assertEquals(2, nodeState.getCurrentTerm());
    org.junit.jupiter.api.Assertions.assertEquals(5, commitIndex.getCommitIndex());
    org.junit.jupiter.api.Assertions.assertEquals(5, stateMachineApplier.getLastApplied());
    org.junit.jupiter.api.Assertions.assertArrayEquals(snapshotData, restoredSnapshotData);

    var persistedSnapshot = snapshotStore.load().orElseThrow();

    org.junit.jupiter.api.Assertions.assertEquals(5, persistedSnapshot.getLastIncludedIndex());
    org.junit.jupiter.api.Assertions.assertEquals(2, persistedSnapshot.getLastIncludedTerm());
    org.junit.jupiter.api.Assertions.assertArrayEquals(snapshotData, persistedSnapshot.getData());

    org.junit.jupiter.api.Assertions.assertEquals(5, logStore.getLastIncludedIndex());
    org.junit.jupiter.api.Assertions.assertEquals(2, logStore.getLastIncludedTerm());
    org.junit.jupiter.api.Assertions.assertEquals(1, timeoutResetCount);
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
    private long lastIncludedIndex;
    private long lastIncludedTerm;

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
      return entries.isEmpty() ? lastIncludedIndex : Math.max(lastIncludedIndex, entries.lastKey());
    }

    @Override
    public long getLastLogTerm() {
      return entries.isEmpty() ? lastIncludedTerm : entries.lastEntry().getValue().getTerm();
    }

    @Override
    public long getLastIncludedIndex() {
      return lastIncludedIndex;
    }

    @Override
    public long getLastIncludedTerm() {
      return lastIncludedTerm;
    }

    @Override
    public Optional<Long> getTerm(long index) {
      if (index == lastIncludedIndex && index > 0) {
        return Optional.of(lastIncludedTerm);
      }

      return getEntry(index).map(RaftLogEntry::getTerm);
    }

    @Override
    public void installSnapshotBoundary(long index, long term) {
      boolean matchingEntry = getEntry(index).map(entry -> entry.getTerm() == term).orElse(false);

      if (matchingEntry) {
        entries.headMap(index, true).clear();
      } else {
        entries.clear();
      }

      lastIncludedIndex = index;
      lastIncludedTerm = term;
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
