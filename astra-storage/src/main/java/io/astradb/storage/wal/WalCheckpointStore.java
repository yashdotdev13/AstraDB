package io.astradb.storage.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Persists WAL checkpoints to disk.
 *
 * <p>Checkpoint data is written to a temporary file and then atomically replaces the previous
 * checkpoint whenever the filesystem supports it.
 */
public final class WalCheckpointStore {

  private static final int MAGIC = 0x41535443;
  private static final int FORMAT_VERSION = 1;
  private static final int CHECKSUM_LENGTH = 32;
  private static final int MAX_SNAPSHOT_SIZE = 1024 * 1024 * 1024;

  private final Path checkpointPath;

  public WalCheckpointStore(Path checkpointPath) {
    this.checkpointPath =
        Objects.requireNonNull(checkpointPath, "Checkpoint path must not be null")
            .toAbsolutePath()
            .normalize();
  }

  /**
   * Saves a checkpoint to disk.
   *
   * @param checkpoint checkpoint to persist
   * @throws IOException if persistence fails
   */
  public synchronized void save(WalCheckpoint checkpoint) throws IOException {
    Objects.requireNonNull(checkpoint, "Checkpoint must not be null");

    Optional<WalCheckpoint> existing = load();

    if (existing.isPresent()
        && checkpoint.getLastIncludedLsn().getValue()
            < existing.get().getLastIncludedLsn().getValue()) {
      throw new IllegalArgumentException("Cannot replace a newer WAL checkpoint");
    }

    byte[] serialized = serialize(checkpoint);

    Path parent = checkpointPath.getParent();

    if (parent != null) {
      Files.createDirectories(parent);
    }

    Path temporaryPath =
        checkpointPath.resolveSibling(checkpointPath.getFileName().toString() + ".tmp");

    try {
      writeAndForce(temporaryPath, serialized);
      replaceCheckpoint(temporaryPath);
    } finally {
      Files.deleteIfExists(temporaryPath);
    }
  }

  /**
   * Loads the persisted checkpoint, if one exists.
   *
   * @return persisted checkpoint
   * @throws IOException if the checkpoint is corrupted or cannot be read
   */
  public synchronized Optional<WalCheckpoint> load() throws IOException {
    if (!Files.exists(checkpointPath)) {
      return Optional.empty();
    }

    byte[] serialized = Files.readAllBytes(checkpointPath);

    return Optional.of(deserialize(serialized));
  }

  /**
   * Deletes the persisted checkpoint.
   *
   * @throws IOException if deletion fails
   */
  public synchronized void delete() throws IOException {
    Files.deleteIfExists(checkpointPath);
  }

  private byte[] serialize(WalCheckpoint checkpoint) throws IOException {
    byte[] snapshotData = checkpoint.getSnapshotData();

    if (snapshotData.length > MAX_SNAPSHOT_SIZE) {
      throw new IOException("Checkpoint snapshot exceeds the maximum supported size");
    }

    ByteArrayOutputStream payloadBuffer = new ByteArrayOutputStream();

    try (DataOutputStream output = new DataOutputStream(payloadBuffer)) {
      output.writeInt(MAGIC);
      output.writeInt(FORMAT_VERSION);
      output.writeLong(checkpoint.getLastIncludedLsn().getValue());
      output.writeInt(snapshotData.length);
      output.write(snapshotData);
    }

    byte[] payload = payloadBuffer.toByteArray();
    byte[] checksum = calculateChecksum(payload);

    ByteArrayOutputStream result = new ByteArrayOutputStream();

    result.write(payload);
    result.write(checksum);

    return result.toByteArray();
  }

  private WalCheckpoint deserialize(byte[] serialized) throws IOException {
    Objects.requireNonNull(serialized, "Serialized checkpoint must not be null");

    if (serialized.length < Integer.BYTES * 3L + Long.BYTES + CHECKSUM_LENGTH) {
      throw new IOException("Checkpoint file is too small");
    }

    int payloadLength = serialized.length - CHECKSUM_LENGTH;

    byte[] payload = Arrays.copyOfRange(serialized, 0, payloadLength);
    byte[] storedChecksum = Arrays.copyOfRange(serialized, payloadLength, serialized.length);

    byte[] calculatedChecksum = calculateChecksum(payload);

    if (!MessageDigest.isEqual(storedChecksum, calculatedChecksum)) {
      throw new IOException("Checkpoint checksum validation failed");
    }

    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {

      int magic = input.readInt();

      if (magic != MAGIC) {
        throw new IOException("Invalid checkpoint file magic number");
      }

      int version = input.readInt();

      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported checkpoint format version: " + version);
      }

      long lastIncludedLsn = input.readLong();

      if (lastIncludedLsn < 0) {
        throw new IOException("Checkpoint LSN cannot be negative");
      }

      int snapshotLength = input.readInt();

      if (snapshotLength < 0 || snapshotLength > MAX_SNAPSHOT_SIZE) {
        throw new IOException("Invalid checkpoint snapshot length");
      }

      if (snapshotLength != input.available()) {
        throw new IOException("Checkpoint snapshot length does not match file contents");
      }

      byte[] snapshotData = new byte[snapshotLength];
      input.readFully(snapshotData);

      if (input.available() != 0) {
        throw new IOException("Unexpected trailing checkpoint data");
      }

      return new WalCheckpoint(LogSequenceNumber.of(lastIncludedLsn), snapshotData);

    } catch (IOException exception) {
      throw new IOException("Failed to deserialize WAL checkpoint", exception);
    }
  }

  private byte[] calculateChecksum(byte[] payload) throws IOException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return digest.digest(payload);
    } catch (NoSuchAlgorithmException exception) {
      throw new IOException("SHA-256 is unavailable", exception);
    }
  }

  private void writeAndForce(Path path, byte[] data) throws IOException {
    try (FileChannel channel =
        FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {

      ByteBuffer buffer = ByteBuffer.wrap(data);

      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }

      channel.force(true);
    }
  }

  private void replaceCheckpoint(Path temporaryPath) throws IOException {
    try {
      Files.move(
          temporaryPath,
          checkpointPath,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException exception) {
      Files.move(temporaryPath, checkpointPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
