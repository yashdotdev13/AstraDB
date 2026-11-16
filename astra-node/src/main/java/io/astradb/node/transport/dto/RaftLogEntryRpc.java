package io.astradb.node.transport.dto;

import io.astradb.raft.log.RaftLogEntry;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.Base64;

public record RaftLogEntryRpc(@Positive long index, @Positive long term, @NotNull String payload) {

  public RaftLogEntry toRaftLogEntry() {
    return new RaftLogEntry(index, term, Base64.getDecoder().decode(payload));
  }

  public static RaftLogEntryRpc from(RaftLogEntry entry) {
    return new RaftLogEntryRpc(
        entry.getIndex(), entry.getTerm(), Base64.getEncoder().encodeToString(entry.getPayload()));
  }
}
