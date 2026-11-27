package io.astradb.queryexecutor.dml;

import io.astradb.queryexecutor.exception.QueryExecutionException;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlTypeName;

/** Executes INSERT statements using transaction-backed relational storage. */
public final class InsertExecutor {

  private final TransactionBackedRelationalTableAccess tableAccess;

  public InsertExecutor(TransactionBackedRelationalTableAccess tableAccess) {
    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");
  }

  public DmlExecutionResult execute(
      SqlPlan plan, SqlSchema schema, Transaction<String, RelationalRow> transaction) {

    Objects.requireNonNull(plan, "SQL plan must not be null");
    Objects.requireNonNull(schema, "SQL schema must not be null");
    Objects.requireNonNull(transaction, "Transaction must not be null");

    if (!(plan.validatedStatement() instanceof SqlInsert insert)) {
      throw new QueryExecutionException("Validated statement is not an INSERT statement");
    }

    SqlTable table = resolveTable(insert, schema);
    List<Integer> targetIndexes = resolveTargetColumns(insert, table);
    List<RelationalRow> rows = buildRows(insert, table, targetIndexes);

    // Stage writes only after every row has passed validation.
    for (RelationalRow row : rows) {
      tableAccess.insert(transaction, table, row);
    }

    return new DmlExecutionResult(rows.size());
  }

  private SqlTable resolveTable(SqlInsert insert, SqlSchema schema) {
    if (!(insert.getTargetTable() instanceof SqlIdentifier identifier)
        || identifier.names.isEmpty()) {
      throw new QueryExecutionException("Invalid INSERT target table");
    }

    String tableName = identifier.names.get(identifier.names.size() - 1);

    return schema
        .findTable(tableName)
        .orElseThrow(() -> new QueryExecutionException("Unknown table: " + tableName));
  }

  private List<Integer> resolveTargetColumns(SqlInsert insert, SqlTable table) {
    SqlNodeList columnList = insert.getTargetColumnList();

    List<Integer> indexes = new ArrayList<>();
    Set<Integer> seenIndexes = new HashSet<>();

    if (columnList == null || columnList.isEmpty()) {
      for (int i = 0; i < table.columns().size(); i++) {
        indexes.add(i);
      }
      return indexes;
    }

    for (SqlNode columnNode : columnList) {
      if (!(columnNode instanceof SqlIdentifier identifier)) {
        throw new QueryExecutionException("Invalid INSERT column");
      }

      String columnName = identifier.getSimple();
      int columnIndex = findColumnIndex(table, columnName);

      if (columnIndex < 0) {
        throw new QueryExecutionException(
            "Unknown column '" + columnName + "' in table '" + table.name() + "'");
      }

      if (!seenIndexes.add(columnIndex)) {
        throw new QueryExecutionException("Duplicate INSERT column: " + columnName);
      }

      indexes.add(columnIndex);
    }

    return indexes;
  }

  private int findColumnIndex(SqlTable table, String columnName) {
    for (int i = 0; i < table.columns().size(); i++) {
      if (table.columns().get(i).name().equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    return -1;
  }

  private List<RelationalRow> buildRows(
      SqlInsert insert, SqlTable table, List<Integer> targetIndexes) {

    SqlNode source = insert.getSource();

    if (!(source instanceof SqlCall valuesCall) || valuesCall.getKind() != SqlKind.VALUES) {
      throw new QueryExecutionException("Only INSERT statements with VALUES are supported");
    }

    List<RelationalRow> rows = new ArrayList<>();

    for (SqlNode rowNode : valuesCall.getOperandList()) {
      if (!(rowNode instanceof SqlCall rowCall) || rowCall.getKind() != SqlKind.ROW) {
        throw new QueryExecutionException("Invalid INSERT VALUES row");
      }

      List<SqlNode> values = rowCall.getOperandList();

      if (values.size() != targetIndexes.size()) {
        throw new QueryExecutionException("INSERT value count does not match target column count");
      }

      Object[] rowValues = new Object[table.columns().size()];

      for (int i = 0; i < values.size(); i++) {
        int targetIndex = targetIndexes.get(i);
        SqlColumn column = table.columns().get(targetIndex);

        rowValues[targetIndex] = convertLiteral(values.get(i), column);
      }

      validateNullability(rowValues, table);
      rows.add(RelationalRow.of(rowValues));
    }

    if (rows.isEmpty()) {
      throw new QueryExecutionException("INSERT contains no VALUES rows");
    }

    return List.copyOf(rows);
  }

  private Object convertLiteral(SqlNode node, SqlColumn column) {
    if (!(node instanceof SqlLiteral literal)) {
      throw new QueryExecutionException("Only literal values are supported in INSERT statements");
    }

    if (literal.getTypeName() == SqlTypeName.NULL) {
      return null;
    }

    String type = normalizeType(column.type());

    try {
      return switch (type) {
        case "INTEGER", "INT" -> numericValue(literal).intValueExact();
        case "BIGINT" -> numericValue(literal).longValueExact();
        case "SMALLINT" -> numericValue(literal).shortValueExact();
        case "TINYINT" -> numericValue(literal).byteValueExact();
        case "DECIMAL", "NUMERIC" -> numericValue(literal);
        case "FLOAT", "DOUBLE" -> numericValue(literal).doubleValue();
        case "REAL" -> numericValue(literal).floatValue();
        case "VARCHAR", "CHAR" -> stringValue(literal);
        case "BOOLEAN" -> booleanValue(literal);
        default ->
            throw new QueryExecutionException("Unsupported INSERT column type: " + column.type());
      };
    } catch (ArithmeticException exception) {
      throw new QueryExecutionException(
          "Value is out of range or has an incompatible numeric format for column '"
              + column.name()
              + "'",
          exception);
    }
  }

  private BigDecimal numericValue(SqlLiteral literal) {
    if (!(literal instanceof SqlNumericLiteral numericLiteral)) {
      throw new QueryExecutionException("Expected a numeric literal");
    }

    return numericLiteral.bigDecimalValue();
  }

  private String stringValue(SqlLiteral literal) {
    if (!(literal instanceof SqlCharStringLiteral charLiteral)) {
      throw new QueryExecutionException("Expected a string literal");
    }

    return charLiteral.getNlsString().getValue();
  }

  private Boolean booleanValue(SqlLiteral literal) {
    if (literal.getTypeName() != SqlTypeName.BOOLEAN) {
      throw new QueryExecutionException("Expected a boolean literal");
    }

    Object value = literal.getValue();

    if (!(value instanceof Boolean booleanResult)) {
      throw new QueryExecutionException("Invalid boolean literal");
    }

    return booleanResult;
  }

  private void validateNullability(Object[] values, SqlTable table) {
    for (int i = 0; i < values.length; i++) {
      SqlColumn column = table.columns().get(i);

      if (values[i] == null && !column.nullable()) {
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
