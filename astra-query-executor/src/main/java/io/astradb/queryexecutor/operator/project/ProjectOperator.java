package io.astradb.queryexecutor.operator.project;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Physical operator that selects columns from rows produced by another operator.
 *
 * <p>Column indexes determine both the selected columns and their output order.
 */
public final class ProjectOperator implements PhysicalOperator {

  private final PhysicalOperator input;
  private final int[] columnIndexes;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  public ProjectOperator(PhysicalOperator input, int... columnIndexes) {
    this.input = Objects.requireNonNull(input, "Input operator must not be null");
    Objects.requireNonNull(columnIndexes, "Column indexes must not be null");

    if (columnIndexes.length == 0) {
      throw new IllegalArgumentException("At least one column index must be specified");
    }

    for (int index : columnIndexes) {
      if (index < 0) {
        throw new IllegalArgumentException("Column indexes must not be negative");
      }
    }

    this.columnIndexes = columnIndexes.clone();
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Project operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Project operator has already been opened");
    }

    RowIterator inputIterator = input.open();
    activeIterator = new ProjectingRowIterator(inputIterator);
    opened = true;

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

    input.close();
  }

  private final class ProjectingRowIterator implements RowIterator {

    private final RowIterator delegate;
    private boolean iteratorClosed;

    private ProjectingRowIterator(RowIterator delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return !iteratorClosed && delegate.hasNext();
    }

    @Override
    public RelationalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more rows available");
      }

      RelationalRow inputRow = delegate.next();
      List<Object> projectedValues = new ArrayList<>(columnIndexes.length);

      for (int columnIndex : columnIndexes) {
        if (columnIndex >= inputRow.size()) {
          close();
          throw new IllegalArgumentException(
              "Column index " + columnIndex + " is outside row size " + inputRow.size());
        }

        projectedValues.add(inputRow.getValue(columnIndex));
      }

      return new RelationalRow(projectedValues);
    }

    @Override
    public void close() {
      if (iteratorClosed) {
        return;
      }

      iteratorClosed = true;
      delegate.close();
    }
  }
}
