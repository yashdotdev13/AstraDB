package io.astradb.queryexecutor.operator.sort;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/** Physical operator that sorts rows using one or more sort keys. */
public final class SortOperator implements PhysicalOperator {

  private final PhysicalOperator input;
  private final List<SortKey> sortKeys;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  public SortOperator(PhysicalOperator input, List<SortKey> sortKeys) {
    this.input = Objects.requireNonNull(input, "Input operator must not be null");
    Objects.requireNonNull(sortKeys, "Sort keys must not be null");

    if (sortKeys.isEmpty()) {
      throw new IllegalArgumentException("At least one sort key must be specified");
    }

    this.sortKeys = List.copyOf(sortKeys);
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Sort operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Sort operator has already been opened");
    }

    opened = true;

    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = input.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    } catch (RuntimeException exception) {
      close();
      throw exception;
    }

    rows.sort(this::compareRows);

    activeIterator = new SortedRowIterator(rows);
    return activeIterator;
  }

  private int compareRows(RelationalRow left, RelationalRow right) {
    for (SortKey sortKey : sortKeys) {
      int index = sortKey.columnIndex();

      if (index >= left.size() || index >= right.size()) {
        throw new IllegalArgumentException("Sort column index " + index + " exceeds row size");
      }

      Object leftValue = left.getValue(index);
      Object rightValue = right.getValue(index);

      int result = compareValues(leftValue, rightValue, sortKey);

      if (result != 0) {
        return result;
      }
    }

    return 0;
  }

  private int compareValues(Object left, Object right, SortKey sortKey) {
    if (left == null || right == null) {
      if (left == null && right == null) {
        return 0;
      }

      return left == null
          ? (sortKey.nullDirection() == SortKey.NullDirection.FIRST ? -1 : 1)
          : (sortKey.nullDirection() == SortKey.NullDirection.FIRST ? 1 : -1);
    }

    int result;

    if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
      result = toBigDecimal(leftNumber).compareTo(toBigDecimal(rightNumber));
    } else if (left instanceof Comparable<?> && left.getClass().isInstance(right)) {
      @SuppressWarnings("unchecked")
      Comparable<Object> comparable = (Comparable<Object>) left;
      result = comparable.compareTo(right);
    } else {
      throw new IllegalArgumentException(
          "Cannot sort values of types "
              + left.getClass().getName()
              + " and "
              + right.getClass().getName());
    }

    return sortKey.direction() == SortKey.Direction.ASCENDING ? result : -result;
  }

  private BigDecimal toBigDecimal(Number number) {
    return number instanceof BigDecimal decimal ? decimal : new BigDecimal(number.toString());
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

    input.close();
  }

  private static final class SortedRowIterator implements RowIterator {

    private final List<RelationalRow> rows;
    private int index;
    private boolean iteratorClosed;

    private SortedRowIterator(List<RelationalRow> rows) {
      this.rows = List.copyOf(rows);
    }

    @Override
    public boolean hasNext() {
      return !iteratorClosed && index < rows.size();
    }

    @Override
    public RelationalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more rows available");
      }

      return rows.get(index++);
    }

    @Override
    public void close() {
      iteratorClosed = true;
    }
  }
}
