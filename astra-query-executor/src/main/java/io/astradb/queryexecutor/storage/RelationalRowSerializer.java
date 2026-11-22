package io.astradb.queryexecutor.storage;

import io.astradb.sql.model.RelationalRow;
import io.astradb.storage.rocksdb.ValueSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Serializes relational rows into a versioned binary representation.
 *
 * <p>Each value is encoded with a type tag to preserve its Java type during deserialization.
 */
public final class RelationalRowSerializer implements ValueSerializer<RelationalRow> {

  private static final int FORMAT_VERSION = 1;

  private static final byte NULL = 0;
  private static final byte STRING = 1;
  private static final byte INTEGER = 2;
  private static final byte LONG = 3;
  private static final byte SHORT = 4;
  private static final byte BYTE = 5;
  private static final byte BOOLEAN = 6;
  private static final byte FLOAT = 7;
  private static final byte DOUBLE = 8;
  private static final byte DECIMAL = 9;
  private static final byte BYTES = 10;
  private static final byte LOCAL_DATE = 11;
  private static final byte LOCAL_TIME = 12;
  private static final byte LOCAL_DATE_TIME = 13;
  private static final byte UUID_TYPE = 14;

  @Override
  public byte[] serialize(RelationalRow row) {
    Objects.requireNonNull(row, "Row must not be null");

    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      try (DataOutputStream output = new DataOutputStream(byteStream)) {
        output.writeInt(FORMAT_VERSION);
        output.writeInt(row.size());

        for (Object value : row.values()) {
          writeValue(output, value);
        }
      }

      return byteStream.toByteArray();
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to serialize relational row", exception);
    }
  }

  @Override
  public RelationalRow deserialize(byte[] bytes) {
    Objects.requireNonNull(bytes, "Serialized bytes must not be null");

    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {

      int version = input.readInt();

      if (version != FORMAT_VERSION) {
        throw new IllegalArgumentException("Unsupported relational row format version: " + version);
      }

      int size = input.readInt();

      if (size < 0) {
        throw new IllegalArgumentException("Invalid relational row size: " + size);
      }

      List<Object> values = new ArrayList<>(size);

      for (int index = 0; index < size; index++) {
        values.add(readValue(input));
      }

      if (input.available() != 0) {
        throw new IllegalArgumentException("Unexpected trailing data in serialized relational row");
      }

      return new RelationalRow(values);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to deserialize relational row", exception);
    }
  }

  private void writeValue(DataOutputStream output, Object value) throws IOException {
    if (value == null) {
      output.writeByte(NULL);
    } else if (value instanceof String string) {
      output.writeByte(STRING);
      writeString(output, string);
    } else if (value instanceof Integer number) {
      output.writeByte(INTEGER);
      output.writeInt(number);
    } else if (value instanceof Long number) {
      output.writeByte(LONG);
      output.writeLong(number);
    } else if (value instanceof Short number) {
      output.writeByte(SHORT);
      output.writeShort(number);
    } else if (value instanceof Byte number) {
      output.writeByte(BYTE);
      output.writeByte(number);
    } else if (value instanceof Boolean bool) {
      output.writeByte(BOOLEAN);
      output.writeBoolean(bool);
    } else if (value instanceof Float number) {
      output.writeByte(FLOAT);
      output.writeFloat(number);
    } else if (value instanceof Double number) {
      output.writeByte(DOUBLE);
      output.writeDouble(number);
    } else if (value instanceof BigDecimal decimal) {
      output.writeByte(DECIMAL);
      writeString(output, decimal.toString());
    } else if (value instanceof byte[] binary) {
      output.writeByte(BYTES);
      output.writeInt(binary.length);
      output.write(binary);
    } else if (value instanceof LocalDate date) {
      output.writeByte(LOCAL_DATE);
      writeString(output, date.toString());
    } else if (value instanceof LocalTime time) {
      output.writeByte(LOCAL_TIME);
      writeString(output, time.toString());
    } else if (value instanceof LocalDateTime dateTime) {
      output.writeByte(LOCAL_DATE_TIME);
      writeString(output, dateTime.toString());
    } else if (value instanceof UUID uuid) {
      output.writeByte(UUID_TYPE);
      output.writeLong(uuid.getMostSignificantBits());
      output.writeLong(uuid.getLeastSignificantBits());
    } else {
      throw new IllegalArgumentException(
          "Unsupported relational value type: " + value.getClass().getName());
    }
  }

  private Object readValue(DataInputStream input) throws IOException {
    byte type = input.readByte();

    return switch (type) {
      case NULL -> null;
      case STRING -> readString(input);
      case INTEGER -> input.readInt();
      case LONG -> input.readLong();
      case SHORT -> input.readShort();
      case BYTE -> input.readByte();
      case BOOLEAN -> input.readBoolean();
      case FLOAT -> input.readFloat();
      case DOUBLE -> input.readDouble();
      case DECIMAL -> new BigDecimal(readString(input));
      case BYTES -> readBytes(input);
      case LOCAL_DATE -> LocalDate.parse(readString(input));
      case LOCAL_TIME -> LocalTime.parse(readString(input));
      case LOCAL_DATE_TIME -> LocalDateTime.parse(readString(input));
      case UUID_TYPE -> new UUID(input.readLong(), input.readLong());
      default -> throw new IllegalArgumentException("Unknown relational value type tag: " + type);
    };
  }

  private void writeString(DataOutputStream output, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private String readString(DataInputStream input) throws IOException {
    return new String(readBytes(input), StandardCharsets.UTF_8);
  }

  private byte[] readBytes(DataInputStream input) throws IOException {
    int length = input.readInt();

    if (length < 0 || length > input.available()) {
      throw new IllegalArgumentException("Invalid serialized value length: " + length);
    }

    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }
}
