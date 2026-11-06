package io.astradb.raft.metadata;

public interface RaftMetadataStore {

  RaftMetadata loadMetadata();

  void saveMetadata(RaftMetadata metadata);
}
