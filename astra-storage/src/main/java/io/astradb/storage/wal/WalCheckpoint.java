package io.astradb.storage.wal;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable representation of a WAL checkpoint.
 *
 * <p>A checkpoint contains the WAL sequence number covered by the snapshot and the serialized MVCC
 * snapshot data.
 */
public final class WalCheckpoint {

  private final LogSequenceNumber lastIncludedLsn;
  private final byte[] snapshotData;

  public WalCheckpoint(LogSequenceNumber lastIncludedLsn, byte[] snapshotData) {
    this.lastIncludedLsn =
        Objects.requireNonNull(lastIncludedLsn, "Last included LSN must not be null");

    this.snapshotData =
        Objects.requireNonNull(snapshotData, "Snapshot data must not be null").clone();
  }

  public LogSequenceNumber getLastIncludedLsn() {
    return lastIncludedLsn;
  }

  public byte[] getSnapshotData() {
    return snapshotData.clone();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }

    if (!(object instanceof WalCheckpoint)) {
      return false;
    }

    WalCheckpoint that = (WalCheckpoint) object;

    return lastIncludedLsn.equals(that.lastIncludedLsn)
        && Arrays.equals(snapshotData, that.snapshotData);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hash(lastIncludedLsn) + Arrays.hashCode(snapshotData);
  }
}
