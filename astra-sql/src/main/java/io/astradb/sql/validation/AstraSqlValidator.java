package io.astradb.sql.validation;

import io.astradb.sql.exception.SqlParseException;
import io.astradb.sql.exception.SqlValidationException;
import io.astradb.sql.model.SqlSchema;
import java.util.Objects;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

/** Validates SQL statements against AstraDB schema metadata. */
public class AstraSqlValidator {

  /**
   * Parses and validates a SQL statement against the supplied schema.
   *
   * @param sql SQL statement
   * @param sqlSchema AstraDB schema metadata
   * @return validated SQL node
   * @throws NullPointerException if the schema is null
   * @throws SqlParseException if the SQL statement contains invalid syntax
   * @throws SqlValidationException if the SQL statement fails semantic validation
   */
  public SqlNode validate(String sql, SqlSchema sqlSchema) {
    Objects.requireNonNull(sqlSchema, "SQL schema must not be null");

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus defaultSchema = rootSchema.add("ASTRA", new CalciteSchemaAdapter(sqlSchema));

    Planner planner =
        Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(defaultSchema).build());

    try {
      SqlNode parsedNode = planner.parse(sql);
      return planner.validate(parsedNode);
    } catch (org.apache.calcite.sql.parser.SqlParseException exception) {
      throw new SqlParseException(
          "Failed to parse SQL statement: " + exception.getMessage(), exception);
    } catch (ValidationException exception) {
      throw new SqlValidationException(
          "SQL validation failed: " + exception.getMessage(), exception);
    } finally {
      planner.close();
    }
  }
}
