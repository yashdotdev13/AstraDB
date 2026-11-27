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
import java.util.*;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.type.SqlTypeName;

/** Executes supported MERGE statements using transaction-backed relational storage. */
public final class MergeExecutor {

  private final TransactionBackedRelationalTableAccess tableAccess;
  private final RexExpressionEvaluator evaluator;

  public MergeExecutor(TransactionBackedRelationalTableAccess tableAccess) {
    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");
    this.evaluator = new RexExpressionEvaluator();
  }

  public DmlExecutionResult execute(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    Objects.requireNonNull(plan, "SQL plan must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");
    Objects.requireNonNull(transaction, "Transaction must not be null");

    if (!(plan.validatedStatement() instanceof SqlMerge merge)) {
      throw new QueryExecutionException("Validated statement is not a MERGE statement");
    }

    if (!(plan.logicalPlan().orElse(null) instanceof TableModify modify)
        || modify.getOperation() != TableModify.Operation.MERGE
        || !(modify.getInput() instanceof LogicalProject project)
        || !(project.getInput() instanceof Join join)
        || !(join.getLeft() instanceof TableScan sourceScan)
        || !(join.getRight() instanceof TableScan targetScan)) {
      throw new QueryExecutionException("Unsupported MERGE relational plan");
    }

    SqlTable targetTable = resolveTable(modify.getTable().getQualifiedName(), schema);
    SqlTable sourceTable = resolveTable(sourceScan.getTable().getQualifiedName(), schema);

    SqlUpdate updateCall = merge.getUpdateCall();
    SqlInsert insertCall = merge.getInsertCall();

    if (updateCall == null && insertCall == null) {
      throw new QueryExecutionException("MERGE must contain an UPDATE or INSERT action");
    }

    List<String> updateColumns =
        updateCall == null ? List.of() : resolveUpdateColumns(updateCall, modify);

    List<Integer> updateIndexes = resolveUpdateIndexes(updateColumns, targetTable);

    List<SqlNode> insertExpressions =
        insertCall == null ? List.of() : resolveInsertExpressions(insertCall);

    List<Integer> insertIndexes =
        insertCall == null ? List.of() : resolveInsertIndexes(insertCall, targetTable);

    if (insertCall != null && insertIndexes.size() != insertExpressions.size()) {
      throw new QueryExecutionException(
          "MERGE INSERT column count does not match expression count");
    }

    int sourceColumnCount = sourceTable.columns().size();
    int targetColumnCount = targetTable.columns().size();
    int expectedProjectCount = sourceColumnCount + targetColumnCount + updateIndexes.size();

    if (project.getProjects().size() != expectedProjectCount) {
      throw new QueryExecutionException(
          "Unsupported MERGE projection: expected "
              + expectedProjectCount
              + " expressions but found "
              + project.getProjects().size());
    }

    Map<String, RelationalRow> targetEntries = tableAccess.scanEntries(transaction, targetTable);
    List<RelationalRow> sourceRows = tableAccess.scan(transaction, sourceTable);

    Map<String, RelationalRow> updates = new LinkedHashMap<>();
    List<RelationalRow> inserts = new ArrayList<>();
    Set<String> alreadyMatchedTargets = new HashSet<>();

    // Prepare all changes before staging any transaction mutations.
    for (RelationalRow sourceRow : sourceRows) {
      List<Map.Entry<String, RelationalRow>> matchingTargets = new ArrayList<>();

      for (Map.Entry<String, RelationalRow> targetEntry : targetEntries.entrySet()) {
        RelationalRow combinedRow = combine(sourceRow, targetEntry.getValue());

        Object matches = evaluator.evaluate(join.getCondition(), combinedRow);

        if (Boolean.TRUE.equals(matches)) {
          matchingTargets.add(targetEntry);
        }
      }

      if (matchingTargets.isEmpty()) {
        if (insertCall != null) {
          inserts.add(
              buildInsertRow(
                  sourceRow, sourceTable, targetTable, insertIndexes, insertExpressions));
        }
        continue;
      }

      if (updateCall == null) {
        continue;
      }

      for (Map.Entry<String, RelationalRow> targetEntry : matchingTargets) {
        String targetKey = targetEntry.getKey();

        if (!alreadyMatchedTargets.add(targetKey)) {
          throw new QueryExecutionException(
              "Multiple source rows matched the same target row in MERGE");
        }

        RelationalRow combinedRow = combine(sourceRow, targetEntry.getValue());
        List<Object> values = new ArrayList<>(targetEntry.getValue().values());

        for (int i = 0; i < updateIndexes.size(); i++) {
          int projectIndex = sourceColumnCount + targetColumnCount + i;
          RexNode expression = project.getProjects().get(projectIndex);

          Object evaluated = evaluator.evaluate(expression, combinedRow);
          SqlColumn column = targetTable.columns().get(updateIndexes.get(i));

          values.set(updateIndexes.get(i), convertValue(evaluated, column));
        }

        validateNullability(values, targetTable);
        updates.put(targetKey, RelationalRow.of(values.toArray()));
      }
    }

    // Stage mutations only after every proposed row has passed validation.
    updates.forEach((key, row) -> tableAccess.update(transaction, targetTable, key, row));

    for (RelationalRow row : inserts) {
      tableAccess.insert(transaction, targetTable, row);
    }

    return new DmlExecutionResult(updates.size() + inserts.size());
  }

  private SqlTable resolveTable(List<String> qualifiedName, SqlSchema schema) {
    if (qualifiedName == null || qualifiedName.isEmpty()) {
      throw new QueryExecutionException("Invalid MERGE table reference");
    }

    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    return schema
        .findTable(tableName)
        .orElseThrow(() -> new QueryExecutionException("Unknown table: " + tableName));
  }

  private List<String> resolveUpdateColumns(SqlUpdate update, TableModify modify) {
    List<String> columns = modify.getUpdateColumnList();

    if (columns == null || columns.size() != update.getTargetColumnList().size()) {
      throw new QueryExecutionException("Invalid MERGE UPDATE column metadata");
    }

    return columns;
  }

  private List<Integer> resolveUpdateIndexes(List<String> columns, SqlTable table) {
    List<Integer> indexes = new ArrayList<>();

    for (String columnName : columns) {
      int index = findColumnIndex(table, columnName);

      if (index < 0) {
        throw new QueryExecutionException(
            "Unknown MERGE UPDATE column '" + columnName + "' in table '" + table.name() + "'");
      }

      if (indexes.contains(index)) {
        throw new QueryExecutionException("Duplicate MERGE UPDATE column: " + columnName);
      }

      indexes.add(index);
    }

    return indexes;
  }

  private List<Integer> resolveInsertIndexes(SqlInsert insert, SqlTable table) {
    SqlNodeList columns = insert.getTargetColumnList();
    List<Integer> indexes = new ArrayList<>();

    if (columns == null || columns.isEmpty()) {
      for (int i = 0; i < table.columns().size(); i++) {
        indexes.add(i);
      }
      return indexes;
    }

    Set<Integer> seen = new HashSet<>();

    for (SqlNode node : columns) {
      if (!(node instanceof SqlIdentifier identifier)) {
        throw new QueryExecutionException("Invalid MERGE INSERT column");
      }

      int index = findColumnIndex(table, identifier.getSimple());

      if (index < 0) {
        throw new QueryExecutionException("Unknown MERGE INSERT column: " + identifier.getSimple());
      }

      if (!seen.add(index)) {
        throw new QueryExecutionException(
            "Duplicate MERGE INSERT column: " + identifier.getSimple());
      }

      indexes.add(index);
    }

    return indexes;
  }

  private List<SqlNode> resolveInsertExpressions(SqlInsert insert) {
    if (!(insert.getSource() instanceof SqlSelect select)) {
      throw new QueryExecutionException(
          "MERGE INSERT currently supports SELECT-based source expressions only");
    }

    SqlNodeList selectList = select.getSelectList();
    List<SqlNode> expressions = new ArrayList<>();

    for (SqlNode node : selectList) {
      expressions.add(node);
    }

    return expressions;
  }

  private RelationalRow buildInsertRow(
      RelationalRow sourceRow,
      SqlTable sourceTable,
      SqlTable targetTable,
      List<Integer> targetIndexes,
      List<SqlNode> expressions) {

    Object[] values = new Object[targetTable.columns().size()];

    for (int i = 0; i < expressions.size(); i++) {
      Object evaluated = evaluateInsertExpression(expressions.get(i), sourceRow, sourceTable);
      int targetIndex = targetIndexes.get(i);

      values[targetIndex] = convertValue(evaluated, targetTable.columns().get(targetIndex));
    }

    validateNullability(Arrays.asList(values), targetTable);
    return RelationalRow.of(values);
  }

  private Object evaluateInsertExpression(
      SqlNode expression, RelationalRow sourceRow, SqlTable sourceTable) {

    if (expression instanceof SqlIdentifier identifier) {
      if (identifier.names.isEmpty()) {
        throw new QueryExecutionException("Invalid MERGE INSERT column reference");
      }

      String columnName = identifier.names.get(identifier.names.size() - 1);

      int sourceIndex = findColumnIndex(sourceTable, columnName);

      if (sourceIndex < 0) {
        throw new QueryExecutionException("Unknown MERGE source column: " + columnName);
      }

      return sourceRow.getValue(sourceIndex);
    }

    if (expression instanceof SqlLiteral literal) {
      if (literal.getTypeName() == SqlTypeName.NULL) {
        return null;
      }

      if (literal instanceof org.apache.calcite.sql.SqlCharStringLiteral charLiteral) {
        return charLiteral.getNlsString().getValue();
      }

      return literal.getValue();
    }

    throw new QueryExecutionException("Unsupported MERGE INSERT expression: " + expression);
  }

  private RelationalRow combine(RelationalRow source, RelationalRow target) {
    List<Object> values = new ArrayList<>(source.values());
    values.addAll(target.values());
    return RelationalRow.of(values.toArray());
  }

  private int findColumnIndex(SqlTable table, String columnName) {
    for (int i = 0; i < table.columns().size(); i++) {
      if (table.columns().get(i).name().equalsIgnoreCase(columnName)) {
        return i;
      }
    }

    return -1;
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
            throw new QueryExecutionException("Unsupported MERGE column type: " + column.type());
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
