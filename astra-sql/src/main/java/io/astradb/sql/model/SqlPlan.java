package io.astradb.sql.model;

import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

/**
 * Represents a validated SQL statement and its logical query plan.
 *
 * <p>The logical plan is optional because relational conversion is not yet supported for every SQL
 * statement type.
 */
public record SqlPlan(
    String sql,
    SqlStatementType statementType,
    SqlNode validatedStatement,
    Optional<RelNode> logicalPlan) {

  public SqlPlan {
    Objects.requireNonNull(sql, "SQL statement must not be null");
    Objects.requireNonNull(statementType, "Statement type must not be null");
    Objects.requireNonNull(validatedStatement, "Validated statement must not be null");
    Objects.requireNonNull(logicalPlan, "Logical plan must not be null");

    if (sql.isBlank()) {
      throw new IllegalArgumentException("SQL statement must not be blank");
    }
  }
}
