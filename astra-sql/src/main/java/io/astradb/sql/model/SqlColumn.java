package io.astradb.sql.model;

import java.util.Objects;

/** Represents a column in a SQL table. */
public record SqlColumn(String name, String type, boolean nullable) {

  public SqlColumn {
    Objects.requireNonNull(name, "Column name must not be null");
    Objects.requireNonNull(type, "Column type must not be null");

    if (name.isBlank()) {
      throw new IllegalArgumentException("Column name must not be blank");
    }

    if (type.isBlank()) {
      throw new IllegalArgumentException("Column type must not be blank");
    }
  }
}
