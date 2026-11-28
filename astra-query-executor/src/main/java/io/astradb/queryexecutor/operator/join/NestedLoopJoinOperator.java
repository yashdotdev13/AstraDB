package io.astradb.queryexecutor.operator.join;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Physical operator that executes INNER and LEFT OUTER JOIN using a nested-loop algorithm.
 *
 * <p>Both input relations are materialized before the join predicate is evaluated. The output row
 * contains the left input values followed by the right input values.
 *
 * <p>For LEFT OUTER JOIN, unmatched left rows are preserved and padded with NULL values for every
 * right-side column.
 */
public final class NestedLoopJoinOperator implements PhysicalOperator {

  private final PhysicalOperator leftInput;
  private final PhysicalOperator rightInput;
  private final Predicate<RelationalRow> condition;
  private final boolean leftOuter;
  private final int rightColumnCount;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  /**
   * Creates an INNER JOIN operator.
   *
   * @param leftInput left input operator
   * @param rightInput right input operator
   * @param condition join predicate
   */
  public NestedLoopJoinOperator(
      PhysicalOperator leftInput, PhysicalOperator rightInput, Predicate<RelationalRow> condition) {
    this(leftInput, rightInput, condition, false, 0);
  }

  /**
   * Creates an INNER JOIN or LEFT OUTER JOIN operator.
   *
   * @param leftInput left input operator
   * @param rightInput right input operator
   * @param condition join predicate
   * @param leftOuter whether unmatched left rows should be preserved
   * @param rightColumnCount number of columns in the right input
   */
  public NestedLoopJoinOperator(
      PhysicalOperator leftInput,
      PhysicalOperator rightInput,
      Predicate<RelationalRow> condition,
      boolean leftOuter,
      int rightColumnCount) {

    this.leftInput = Objects.requireNonNull(leftInput, "Left input must not be null");
    this.rightInput = Objects.requireNonNull(rightInput, "Right input must not be null");
    this.condition = Objects.requireNonNull(condition, "Join condition must not be null");

    if (rightColumnCount < 0) {
      throw new IllegalArgumentException("Right column count must not be negative");
    }

    this.leftOuter = leftOuter;
    this.rightColumnCount = rightColumnCount;
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Nested loop join operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Nested loop join operator has already been opened");
    }

    opened = true;

    List<RelationalRow> leftRows = readAll(leftInput);
    List<RelationalRow> rightRows = readAll(rightInput);
    List<RelationalRow> result = new ArrayList<>();

    for (RelationalRow leftRow : leftRows) {
      boolean matched = false;

      for (RelationalRow rightRow : rightRows) {
        RelationalRow combinedRow = combine(leftRow, rightRow);

        // SQL JOIN predicates match only when the condition evaluates to TRUE.
        if (condition.test(combinedRow)) {
          result.add(combinedRow);
          matched = true;
        }
      }

      if (leftOuter && !matched) {
        result.add(combineWithNulls(leftRow));
      }
    }

    activeIterator = new ListRowIterator(result);
    return activeIterator;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    if (activeIterator != null) {
      activeIterator.close();
    }

    leftInput.close();
    rightInput.close();
  }

  private List<RelationalRow> readAll(PhysicalOperator input) {
    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = input.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }

    return List.copyOf(rows);
  }

  private RelationalRow combine(RelationalRow leftRow, RelationalRow rightRow) {
    List<Object> values = new ArrayList<>(leftRow.size() + rightRow.size());

    values.addAll(leftRow.values());
    values.addAll(rightRow.values());

    return new RelationalRow(values);
  }

  private RelationalRow combineWithNulls(RelationalRow leftRow) {
    List<Object> values = new ArrayList<>(leftRow.size() + rightColumnCount);

    values.addAll(leftRow.values());

    for (int i = 0; i < rightColumnCount; i++) {
      values.add(null);
    }

    return new RelationalRow(values);
  }

  private static final class ListRowIterator implements RowIterator {

    private final java.util.Iterator<RelationalRow> delegate;
    private boolean closed;

    private ListRowIterator(List<RelationalRow> rows) {
      this.delegate = rows.iterator();
    }

    @Override
    public boolean hasNext() {
      return !closed && delegate.hasNext();
    }

    @Override
    public RelationalRow next() {
      if (closed || !delegate.hasNext()) {
        throw new NoSuchElementException("No more rows available");
      }

      return delegate.next();
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
