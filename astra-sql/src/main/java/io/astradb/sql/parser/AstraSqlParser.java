package io.astradb.sql.parser;

import io.astradb.sql.exception.SqlParseException;
import java.util.Objects;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

/** Parses SQL statements using Apache Calcite. */
public class AstraSqlParser {

  /**
   * Parses a SQL statement into a Calcite SQL abstract syntax tree.
   *
   * @param sql SQL statement to parse
   * @return parsed SQL node
   * @throws IllegalArgumentException if the SQL input is null or blank
   * @throws SqlParseException if the SQL statement contains invalid syntax
   */
  public SqlNode parse(String sql) {
    Objects.requireNonNull(sql, "SQL statement must not be null");

    if (sql.isBlank()) {
      throw new IllegalArgumentException("SQL statement must not be blank");
    }

    try {
      return SqlParser.create(sql).parseStmt();
    } catch (org.apache.calcite.sql.parser.SqlParseException exception) {
      throw new SqlParseException(
          "Failed to parse SQL statement: " + exception.getMessage(), exception);
    }
  }
}
