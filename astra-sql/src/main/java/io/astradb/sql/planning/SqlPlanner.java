package io.astradb.sql.planning;

import io.astradb.sql.exception.SqlParseException;
import io.astradb.sql.exception.SqlPlanningException;
import io.astradb.sql.exception.SqlValidationException;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlStatementType;
import io.astradb.sql.validation.CalciteSchemaAdapter;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

/** Creates logical query plans from validated SQL statements. */
public class SqlPlanner {

  /**
   * Validates a SQL statement and generates its logical query plan when supported.
   *
   * @param sql SQL statement
   * @param schema schema metadata used for validation
   * @return generated SQL plan
   */
  public SqlPlan plan(String sql, SqlSchema schema) {
    Objects.requireNonNull(sql, "SQL statement must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus defaultSchema = rootSchema.add("ASTRA", new CalciteSchemaAdapter(schema));

    Planner planner =
        Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(defaultSchema).build());

    try {
      SqlNode parsedStatement = planner.parse(sql);
      SqlNode validatedStatement = planner.validate(parsedStatement);

      SqlStatementType statementType =
          new io.astradb.sql.parser.SqlStatementClassifier().classify(validatedStatement);

      Optional<RelNode> logicalPlan = Optional.empty();

      if (statementType == SqlStatementType.SELECT
          || statementType == SqlStatementType.UPDATE
          || statementType == SqlStatementType.DELETE
          || statementType == SqlStatementType.MERGE) {
        logicalPlan = Optional.of(planner.rel(validatedStatement).rel);
      }
      return new SqlPlan(sql, statementType, validatedStatement, logicalPlan);
    } catch (org.apache.calcite.sql.parser.SqlParseException exception) {
      throw new SqlParseException(
          "Failed to parse SQL statement: " + exception.getMessage(), exception);
    } catch (ValidationException exception) {
      throw new SqlValidationException(
          "SQL validation failed: " + exception.getMessage(), exception);
    } catch (org.apache.calcite.tools.RelConversionException exception) {
      throw new SqlPlanningException(
          "Failed to generate logical SQL plan: " + exception.getMessage(), exception);
    } finally {
      planner.close();
    }
  }
}
