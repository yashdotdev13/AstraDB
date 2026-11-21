package io.astradb.sql.model;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Represents the schema catalog used during SQL validation. */
public final class SqlSchema {

  private final Map<String, SqlTable> tables;

  public SqlSchema(Collection<SqlTable> tables) {
    Objects.requireNonNull(tables, "Tables must not be null");

    Map<String, SqlTable> tableMap = new LinkedHashMap<>();

    for (SqlTable table : tables) {
      Objects.requireNonNull(table, "Table must not be null");

      String key = normalize(table.name());

      if (tableMap.putIfAbsent(key, table) != null) {
        throw new IllegalArgumentException("Duplicate table name: " + table.name());
      }
    }

    this.tables = Collections.unmodifiableMap(tableMap);
  }

  /**
   * Finds a table by name.
   *
   * @param tableName table name
   * @return matching table, if present
   */
  public Optional<SqlTable> findTable(String tableName) {
    Objects.requireNonNull(tableName, "Table name must not be null");
    return Optional.ofNullable(tables.get(normalize(tableName)));
  }

  /**
   * Finds a column within a table.
   *
   * @param tableName table name
   * @param columnName column name
   * @return matching column, if present
   */
  public Optional<SqlColumn> findColumn(String tableName, String columnName) {
    Objects.requireNonNull(columnName, "Column name must not be null");

    return findTable(tableName)
        .flatMap(
            table ->
                table.columns().stream()
                    .filter(column -> normalize(column.name()).equals(normalize(columnName)))
                    .findFirst());
  }

  /**
   * Returns all registered tables.
   *
   * @return immutable collection of tables
   */
  public Collection<SqlTable> getTables() {
    return tables.values();
  }

  private static String normalize(String name) {
    return name.toUpperCase(java.util.Locale.ROOT);
  }
}
