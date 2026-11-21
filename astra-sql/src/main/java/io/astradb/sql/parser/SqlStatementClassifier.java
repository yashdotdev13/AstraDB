package io.astradb.sql.parser;

import io.astradb.sql.model.SqlStatementType;
import java.util.Objects;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/** Classifies parsed SQL statements based on their Calcite SQL kind. */
public class SqlStatementClassifier {

  /**
   * Determines the statement type of a parsed SQL node.
   *
   * @param sqlNode parsed SQL statement
   * @return classified SQL statement type
   */
  public SqlStatementType classify(SqlNode sqlNode) {
    Objects.requireNonNull(sqlNode, "SQL node must not be null");

    SqlKind kind = sqlNode.getKind();

    switch (kind) {
      case SELECT:
        return SqlStatementType.SELECT;
      case INSERT:
        return SqlStatementType.INSERT;
      case UPDATE:
        return SqlStatementType.UPDATE;
      case DELETE:
        return SqlStatementType.DELETE;
      case MERGE:
        return SqlStatementType.MERGE;
      default:
        return SqlStatementType.OTHER;
    }
  }
}
