package io.astradb.queryexecutor.operator.project;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public final class ExpressionProjectOperator implements PhysicalOperator {

  private final PhysicalOperator input;
  private final List<Function<RelationalRow, Object>> projections;

  private boolean opened;
  private boolean closed;

  public ExpressionProjectOperator(
      PhysicalOperator input, List<Function<RelationalRow, Object>> projections) {

    this.input = Objects.requireNonNull(input, "Input operator must not be null");
    Objects.requireNonNull(projections, "Projections must not be null");

    if (projections.isEmpty()) {
      throw new IllegalArgumentException("At least one projection is required");
    }

    this.projections = List.copyOf(projections);
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

    RowIterator inputIterator = input.open();

    return new RowIterator() {
      private boolean iteratorClosed;

      @Override
      public boolean hasNext() {
        if (iteratorClosed) {
          return false;
        }

        boolean hasNext = inputIterator.hasNext();

        if (!hasNext) {
          close();
        }

        return hasNext;
      }

      @Override
      public RelationalRow next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException();
        }

        RelationalRow inputRow = inputIterator.next();
        List<Object> projectedValues = new ArrayList<>(projections.size());

        for (Function<RelationalRow, Object> projection : projections) {
          projectedValues.add(projection.apply(inputRow));
        }

        return new RelationalRow(projectedValues);
      }

      @Override
      public void close() {
        if (!iteratorClosed) {
          iteratorClosed = true;
          inputIterator.close();
        }
      }
    };
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      input.close();
    }
  }
}
