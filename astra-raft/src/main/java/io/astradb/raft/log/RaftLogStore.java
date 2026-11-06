package io.astradb.raft.log;

import java.util.List;
import java.util.Optional;

/**
 * Defines persistent storage operations for the Raft replicated log.
 */
public interface RaftLogStore extends AutoCloseable {

    /**
     * Appends the next entry to the log.
     */
    void append(RaftLogEntry entry);

    /**
     * Atomically appends a sequence of consecutive entries.
     */
    void appendAll(List<RaftLogEntry> entries);

    /**
     * Retrieves an entry by its log index.
     */
    Optional<RaftLogEntry> getEntry(long index);

    /**
     * Retrieves entries in the inclusive range [fromIndex, toIndex].
     */
    List<RaftLogEntry> getEntries(long fromIndex, long toIndex);

    /**
     * Returns the last log index, or zero when the log is empty.
     */
    long getLastLogIndex();

    /**
     * Returns the term of the last log entry, or zero when the log is empty.
     */
    long getLastLogTerm();

    /**
     * Removes all entries whose indexes are greater than or equal to fromIndex.
     */
    void truncateSuffix(long fromIndex);

    /**
     * Flushes pending writes.
     */
    void flush();

    @Override
    void close();
}