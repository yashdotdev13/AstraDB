package io.astradb.sql.model;

import java.util.List;
import java.util.Objects;

/** Represents a table in the SQL schema. */
public record SqlTable(String name, List<SqlColumn> columns) {

  public SqlTable {
    Objects.requireNonNull(name, "Table name must not be null");
    Objects.requireNonNull(columns, "Table columns must not be null");

    if (name.isBlank()) {
      throw new IllegalArgumentException("Table name must not be blank");
    }

    if (columns.isEmpty()) {
      throw new IllegalArgumentException("Table must contain at least one column");
    }

    columns = List.copyOf(columns);
  }
}
