package io.astradb.queryexecutor.plan;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.TableScanOperator;
import io.astradb.queryexecutor.operator.filter.FilterOperator;
import io.astradb.queryexecutor.operator.limit.LimitOperator;
import io.astradb.queryexecutor.operator.limit.OffsetOperator;
import io.astradb.queryexecutor.operator.project.ExpressionProjectOperator;
import io.astradb.queryexecutor.operator.project.ProjectOperator;
import io.astradb.queryexecutor.operator.sort.SortKey;
import io.astradb.queryexecutor.operator.sort.SortOperator;
import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlPlan;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

/** Translates Calcite logical relational plans into AstraDB physical operators. */
public final class PhysicalPlanTranslator {

  private final TransactionBackedRelationalTableAccess tableAccess;
  private final Transaction<String, RelationalRow> transaction;
  private final SqlSchema schema;
  private final RexExpressionEvaluator expressionEvaluator;

  public PhysicalPlanTranslator(
      TransactionBackedRelationalTableAccess tableAccess,
      Transaction<String, RelationalRow> transaction,
      SqlSchema schema) {

    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");
    this.transaction = Objects.requireNonNull(transaction, "Transaction must not be null");
    this.schema = Objects.requireNonNull(schema, "Schema must not be null");
    this.expressionEvaluator = new RexExpressionEvaluator();
  }

  /**
   * Translates a SQL plan and removes internal columns added for sorting.
   *
   * @param sqlPlan validated SQL plan
   * @return physical operator tree producing only SQL-visible columns
   */
  public PhysicalOperator translate(SqlPlan sqlPlan) {
    Objects.requireNonNull(sqlPlan, "SQL plan must not be null");

    RelNode logicalPlan =
        sqlPlan
            .logicalPlan()
            .orElseThrow(
                () -> new IllegalArgumentException("SQL plan does not contain a logical plan"));

    PhysicalOperator physicalPlan = translate(logicalPlan);

    if (!(logicalPlan instanceof Sort)) {
      return physicalPlan;
    }

    SqlNode validatedStatement = sqlPlan.validatedStatement();

    if (!(validatedStatement instanceof SqlSelect select)) {
      physicalPlan.close();
      throw new UnsupportedOperationException(
          "Only SELECT statements are supported for SQL result projection");
    }

    int visibleColumnCount = select.getSelectList().size();
    int physicalColumnCount = logicalPlan.getRowType().getFieldCount();

    if (visibleColumnCount > physicalColumnCount) {
      physicalPlan.close();
      throw new IllegalStateException(
          "SQL projection contains more columns than the physical plan");
    }

    if (visibleColumnCount == 0) {
      physicalPlan.close();
      throw new UnsupportedOperationException("Empty SQL projections are not supported");
    }

    if (visibleColumnCount == physicalColumnCount) {
      return physicalPlan;
    }

    int[] visibleIndexes = new int[visibleColumnCount];

    for (int i = 0; i < visibleColumnCount; i++) {
      visibleIndexes[i] = i;
    }

    return new ProjectOperator(physicalPlan, visibleIndexes);
  }

  /**
   * Translates a Calcite relational plan into a physical operator tree.
   *
   * @param logicalPlan Calcite logical plan
   * @return physical operator tree
   */
  public PhysicalOperator translate(RelNode logicalPlan) {
    Objects.requireNonNull(logicalPlan, "Logical plan must not be null");

    if (logicalPlan instanceof TableScan tableScan) {
      return translateTableScan(tableScan);
    }

    if (logicalPlan instanceof Filter filter) {
      return translateFilter(filter);
    }

    if (logicalPlan instanceof Project project) {
      return translateProject(project);
    }

    if (logicalPlan instanceof Sort sort) {
      return translateSort(sort);
    }

    throw new UnsupportedOperationException(
        "Unsupported relational node: " + logicalPlan.getClass().getName());
  }

  private PhysicalOperator translateTableScan(TableScan tableScan) {
    List<String> qualifiedName = tableScan.getTable().getQualifiedName();

    if (qualifiedName.isEmpty()) {
      throw new IllegalArgumentException("Table scan does not contain a qualified table name");
    }

    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    SqlTable table =
        schema
            .findTable(tableName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Table not found in AstraDB schema: " + tableName));

    return new TableScanOperator(tableAccess, transaction, table);
  }

  private PhysicalOperator translateFilter(Filter filter) {
    PhysicalOperator input = translate(filter.getInput());
    RexNode condition = filter.getCondition();

    return new FilterOperator(
        input, row -> Boolean.TRUE.equals(expressionEvaluator.evaluate(condition, row)));
  }

  private PhysicalOperator translateProject(Project project) {
    PhysicalOperator input = translate(project.getInput());

    List<Function<RelationalRow, Object>> projections =
        project.getProjects().stream()
            .map(
                expression ->
                    (Function<RelationalRow, Object>)
                        row -> expressionEvaluator.evaluate(expression, row))
            .toList();

    return new ExpressionProjectOperator(input, projections);
  }

  private PhysicalOperator translateSort(Sort sort) {
    // Validate LIMIT and OFFSET before creating child operators.
    Long offset = extractRowCount(sort.offset, "OFFSET");
    Long fetch = extractRowCount(sort.fetch, "LIMIT");

    List<SortKey> sortKeys = new ArrayList<>();

    for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {

      SortKey.Direction direction =
          switch (fieldCollation.getDirection()) {
            case ASCENDING, STRICTLY_ASCENDING -> SortKey.Direction.ASCENDING;
            case DESCENDING, STRICTLY_DESCENDING -> SortKey.Direction.DESCENDING;
            default ->
                throw new UnsupportedOperationException(
                    "Unsupported sort direction: " + fieldCollation.getDirection());
          };

      SortKey.NullDirection nullDirection =
          switch (fieldCollation.nullDirection) {
            case FIRST -> SortKey.NullDirection.FIRST;
            case LAST -> SortKey.NullDirection.LAST;
            case UNSPECIFIED ->
                direction == SortKey.Direction.ASCENDING
                    ? SortKey.NullDirection.LAST
                    : SortKey.NullDirection.FIRST;
          };

      sortKeys.add(new SortKey(fieldCollation.getFieldIndex(), direction, nullDirection));
    }

    PhysicalOperator input = translate(sort.getInput());

    // ORDER BY is optional for a SQL LIMIT/OFFSET query.
    if (!sortKeys.isEmpty()) {
      input = new SortOperator(input, sortKeys);
    }

    // Apply OFFSET before LIMIT.
    if (offset != null) {
      input = new OffsetOperator(input, offset);
    }

    if (fetch != null) {
      input = new LimitOperator(input, fetch);
    }

    return input;
  }

  /**
   * Extracts a non-negative numeric literal from a Calcite row-count expression.
   *
   * @param expression Calcite OFFSET or FETCH expression
   * @param clause SQL clause name used in error messages
   * @return extracted value, or null when the clause is absent
   */
  private Long extractRowCount(RexNode expression, String clause) {
    if (expression == null) {
      return null;
    }

    if (!(expression instanceof RexLiteral literal)) {
      throw new UnsupportedOperationException(
          clause + " currently supports only numeric literal values");
    }

    Object value = literal.getValue();

    if (!(value instanceof Number number)) {
      throw new IllegalArgumentException(clause + " must be a numeric literal");
    }

    try {
      long rowCount = new BigDecimal(number.toString()).longValueExact();

      if (rowCount < 0) {
        throw new IllegalArgumentException(clause + " must not be negative");
      }

      return rowCount;
    } catch (NumberFormatException | ArithmeticException exception) {
      throw new IllegalArgumentException(
          clause + " must be a valid non-negative integer", exception);
    }
  }
}
