package io.astradb.queryexecutor.operator.aggregate;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class AggregateOperator implements PhysicalOperator {

  private static final MathContext AVG_CONTEXT = MathContext.DECIMAL128;

  private final PhysicalOperator input;
  private final int[] groupByColumns;
  private final List<AggregateCall> aggregateCalls;

  private boolean opened;
  private boolean closed;

  public AggregateOperator(
      PhysicalOperator input, int[] groupByColumns, List<AggregateCall> aggregateCalls) {

    this.input = Objects.requireNonNull(input, "Input operator must not be null");
    Objects.requireNonNull(groupByColumns, "Group-by columns must not be null");
    Objects.requireNonNull(aggregateCalls, "Aggregate calls must not be null");

    this.groupByColumns = groupByColumns.clone();
    this.aggregateCalls = List.copyOf(aggregateCalls);

    for (int columnIndex : this.groupByColumns) {
      if (columnIndex < 0) {
        throw new IllegalArgumentException("Group-by column indexes must be non-negative");
      }
    }

    for (int i = 0; i < this.groupByColumns.length; i++) {
      for (int j = i + 1; j < this.groupByColumns.length; j++) {
        if (this.groupByColumns[i] == this.groupByColumns[j]) {
          throw new IllegalArgumentException(
              "Duplicate group-by column index: " + this.groupByColumns[i]);
        }
      }
    }

    if (this.aggregateCalls.isEmpty()) {
      throw new IllegalArgumentException("At least one aggregate call is required");
    }
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Operator can only be opened once");
    }

    opened = true;

    List<RelationalRow> results = new ArrayList<>();

    try {
      Map<List<Object>, GroupState> groups = new LinkedHashMap<>();

      RowIterator iterator = input.open();

      try {
        while (iterator.hasNext()) {
          RelationalRow row = iterator.next();
          List<Object> groupKey = createGroupKey(row);

          GroupState state =
              groups.computeIfAbsent(groupKey, ignored -> new GroupState(groupKey, aggregateCalls));

          state.add(row);
        }
      } finally {
        iterator.close();
      }

      // SQL global aggregation returns one row even for empty input.
      if (groupByColumns.length == 0 && groups.isEmpty()) {
        groups.put(List.of(), new GroupState(List.of(), aggregateCalls));
      }

      for (GroupState state : groups.values()) {
        results.add(state.toRow());
      }

      return new ListRowIterator(results);
    } catch (RuntimeException | Error exception) {
      close();
      throw exception;
    }
  }

  private List<Object> createGroupKey(RelationalRow row) {
    List<Object> key = new ArrayList<>(groupByColumns.length);

    for (int columnIndex : groupByColumns) {
      key.add(readColumn(row, columnIndex));
    }

    return java.util.Collections.unmodifiableList(key);
  }

  private static Object readColumn(RelationalRow row, int columnIndex) {
    if (columnIndex >= row.size()) {
      throw new IllegalArgumentException(
          "Column index " + columnIndex + " is outside row size " + row.size());
    }

    return row.getValue(columnIndex);
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      input.close();
    }
  }

  private static final class GroupState {

    private final List<Object> groupKey;
    private final List<Accumulator> accumulators;

    private GroupState(List<Object> groupKey, List<AggregateCall> aggregateCalls) {

      this.groupKey = groupKey;
      this.accumulators = aggregateCalls.stream().map(Accumulator::new).toList();
    }

    private void add(RelationalRow row) {
      for (Accumulator accumulator : accumulators) {
        accumulator.add(row);
      }
    }

    private RelationalRow toRow() {
      List<Object> values = new ArrayList<>(groupKey.size() + accumulators.size());

      values.addAll(groupKey);

      for (Accumulator accumulator : accumulators) {
        values.add(accumulator.result());
      }

      return new RelationalRow(values);
    }
  }

  private static final class Accumulator {

    private final AggregateCall call;

    private long count;
    private BigDecimal sum;
    private Object extremum;

    private Accumulator(AggregateCall call) {
      this.call = call;
    }

    private void add(RelationalRow row) {
      if (call.function() == AggregateFunction.COUNT && call.inputColumnIndex() == -1) {
        count++;
        return;
      }

      Object value = readColumn(row, call.inputColumnIndex());

      if (value == null) {
        return;
      }

      switch (call.function()) {
        case COUNT -> count++;
        case SUM -> {
          BigDecimal number = toBigDecimal(value);
          sum = sum == null ? number : sum.add(number);
          count++;
        }
        case AVG -> {
          BigDecimal number = toBigDecimal(value);
          sum = sum == null ? number : sum.add(number);
          count++;
        }
        case MIN -> {
          if (extremum == null || compareValues(value, extremum) < 0) {
            extremum = value;
          }
        }
        case MAX -> {
          if (extremum == null || compareValues(value, extremum) > 0) {
            extremum = value;
          }
        }
      }
    }

    private Object result() {
      return switch (call.function()) {
        case COUNT -> count;
        case SUM -> sum;
        case AVG -> count == 0 ? null : sum.divide(BigDecimal.valueOf(count), AVG_CONTEXT);
        case MIN, MAX -> extremum;
      };
    }

    private static BigDecimal toBigDecimal(Object value) {
      if (!(value instanceof Number number)) {
        throw new IllegalArgumentException(
            "SUM and AVG require numeric values, got: " + value.getClass().getName());
      }

      try {
        return new BigDecimal(number.toString());
      } catch (NumberFormatException exception) {
        throw new IllegalArgumentException(
            "Invalid numeric value for aggregation: " + value, exception);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareValues(Object left, Object right) {
      if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
        return toBigDecimal(leftNumber).compareTo(toBigDecimal(rightNumber));
      }

      if (left.getClass().isInstance(right) && left instanceof Comparable comparable) {
        return comparable.compareTo(right);
      }

      throw new IllegalArgumentException(
          "Cannot compare aggregate values of types "
              + left.getClass().getName()
              + " and "
              + right.getClass().getName());
    }
  }

  private static final class ListRowIterator implements RowIterator {

    private final Iterator<RelationalRow> delegate;
    private boolean iteratorClosed;

    private ListRowIterator(List<RelationalRow> rows) {
      this.delegate = rows.iterator();
    }

    @Override
    public boolean hasNext() {
      return !iteratorClosed && delegate.hasNext();
    }

    @Override
    public RelationalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return delegate.next();
    }

    @Override
    public void close() {
      iteratorClosed = true;
    }
  }
}
