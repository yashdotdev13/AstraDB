package io.astradb.queryexecutor.dml;

import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.plan.RexExpressionEvaluator;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

/** Executes UPDATE statements using transaction-backed relational storage. */
public final class UpdateExecutor {

  private final TransactionBackedRelationalTableAccess tableAccess;
  private final RexExpressionEvaluator evaluator;

  public UpdateExecutor(TransactionBackedRelationalTableAccess tableAccess) {
    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");
    this.evaluator = new RexExpressionEvaluator();
  }

  public DmlExecutionResult execute(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    Objects.requireNonNull(plan, "SQL plan must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");
    Objects.requireNonNull(transaction, "Transaction must not be null");

    if (!(plan.logicalPlan().orElse(null) instanceof TableModify modify)
        || modify.getOperation() != TableModify.Operation.UPDATE) {
      throw new QueryExecutionException("UPDATE logical plan is missing or invalid");
    }

    SqlTable table = resolveTable(modify, schema);
    Filter filter = resolveFilter(modify);
    List<String> updateColumns = modify.getUpdateColumnList();
    List<RexNode> sourceExpressions = modify.getSourceExpressionList();

    if (updateColumns.size() != sourceExpressions.size()) {
      throw new QueryExecutionException(
          "UPDATE column count does not match source expression count");
    }

    List<Integer> updateIndexes = resolveUpdateIndexes(updateColumns, table);

    // Build and validate all replacements before staging any writes.
    Map<String, RelationalRow> replacements = new LinkedHashMap<>();

    for (Map.Entry<String, RelationalRow> entry :
        tableAccess.scanEntries(transaction, table).entrySet()) {

      RelationalRow existingRow = entry.getValue();

      if (filter != null) {
        Object matches = evaluator.evaluate(filter.getCondition(), existingRow);

        // SQL WHERE selects only rows for which the condition is TRUE.
        if (!Boolean.TRUE.equals(matches)) {
          continue;
        }
      }

      List<Object> values = new ArrayList<>(existingRow.values());

      for (int i = 0; i < updateIndexes.size(); i++) {
        int columnIndex = updateIndexes.get(i);
        SqlColumn column = table.columns().get(columnIndex);

        Object evaluated = evaluator.evaluate(sourceExpressions.get(i), existingRow);

        values.set(columnIndex, convertValue(evaluated, column));
      }

      validateNullability(values, table);

      replacements.put(entry.getKey(), RelationalRow.of(values.toArray()));
    }

    // Preserve existing row keys. The caller controls commit and rollback.
    replacements.forEach((key, row) -> tableAccess.update(transaction, table, key, row));

    return new DmlExecutionResult(replacements.size());
  }

  private SqlTable resolveTable(TableModify modify, SqlSchema schema) {
    List<String> qualifiedName = modify.getTable().getQualifiedName();

    if (qualifiedName.isEmpty()) {
      throw new QueryExecutionException("Invalid UPDATE target table");
    }

    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    return schema
        .findTable(tableName)
        .orElseThrow(() -> new QueryExecutionException("Unknown table: " + tableName));
  }

  private Filter resolveFilter(TableModify modify) {
    RelNode input = modify.getInput();

    if (!(input instanceof Project project)) {
      throw new QueryExecutionException("Unsupported UPDATE relational plan: expected Project");
    }

    RelNode projectInput = project.getInput();

    if (projectInput instanceof Filter filter) {
      if (!(filter.getInput() instanceof TableScan)) {
        throw new QueryExecutionException(
            "Unsupported UPDATE relational plan: expected TableScan below Filter");
      }
      return filter;
    }

    if (projectInput instanceof TableScan) {
      return null;
    }

    throw new QueryExecutionException(
        "Unsupported UPDATE relational plan: expected Filter or TableScan below Project");
  }

  private List<Integer> resolveUpdateIndexes(List<String> updateColumns, SqlTable table) {
    List<Integer> indexes = new ArrayList<>();

    for (String columnName : updateColumns) {
      int index = -1;

      for (int i = 0; i < table.columns().size(); i++) {
        if (table.columns().get(i).name().equalsIgnoreCase(columnName)) {
          index = i;
          break;
        }
      }

      if (index < 0) {
        throw new QueryExecutionException(
            "Unknown UPDATE column '" + columnName + "' in table '" + table.name() + "'");
      }

      if (indexes.contains(index)) {
        throw new QueryExecutionException("Duplicate UPDATE column: " + columnName);
      }

      indexes.add(index);
    }

    return indexes;
  }

  private Object convertValue(Object value, SqlColumn column) {
    if (value == null) {
      return null;
    }

    String type = normalizeType(column.type());

    try {
      return switch (type) {
        case "INTEGER", "INT" -> numericValue(value).intValueExact();
        case "BIGINT" -> numericValue(value).longValueExact();
        case "SMALLINT" -> numericValue(value).shortValueExact();
        case "TINYINT" -> numericValue(value).byteValueExact();
        case "DECIMAL", "NUMERIC" -> numericValue(value);
        case "FLOAT", "DOUBLE" -> numericValue(value).doubleValue();
        case "REAL" -> numericValue(value).floatValue();
        case "VARCHAR", "CHAR" -> {
          if (!(value instanceof String)) {
            throw new QueryExecutionException(
                "Expected a string value for column '" + column.name() + "'");
          }
          yield value;
        }
        case "BOOLEAN" -> {
          if (!(value instanceof Boolean)) {
            throw new QueryExecutionException(
                "Expected a boolean value for column '" + column.name() + "'");
          }
          yield value;
        }
        default ->
            throw new QueryExecutionException("Unsupported UPDATE column type: " + column.type());
      };
    } catch (ArithmeticException exception) {
      throw new QueryExecutionException(
          "Value is out of range or has an incompatible numeric format for column '"
              + column.name()
              + "'",
          exception);
    }
  }

  private BigDecimal numericValue(Object value) {
    if (value instanceof Number number) {
      return new BigDecimal(number.toString());
    }

    throw new QueryExecutionException("Expected a numeric value");
  }

  private void validateNullability(List<Object> values, SqlTable table) {
    for (int i = 0; i < values.size(); i++) {
      SqlColumn column = table.columns().get(i);

      if (values.get(i) == null && !column.nullable()) {
        throw new QueryExecutionException("Column '" + column.name() + "' cannot be NULL");
      }
    }
  }

  private String normalizeType(String type) {
    int precisionIndex = type.indexOf('(');
    String normalized = precisionIndex >= 0 ? type.substring(0, precisionIndex) : type;

    return normalized.trim().toUpperCase(Locale.ROOT);
  }
}
