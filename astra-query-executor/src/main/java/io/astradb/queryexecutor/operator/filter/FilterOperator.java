package io.astradb.queryexecutor.operator.filter;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Physical operator that filters rows produced by another operator.
 *
 * <p>Rows are evaluated lazily and emitted only when they match the predicate.
 */
public final class FilterOperator implements PhysicalOperator {

  private final PhysicalOperator input;
  private final Predicate<RelationalRow> predicate;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  public FilterOperator(PhysicalOperator input, Predicate<RelationalRow> predicate) {
    this.input = Objects.requireNonNull(input, "Input operator must not be null");
    this.predicate = Objects.requireNonNull(predicate, "Predicate must not be null");
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Filter operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Filter operator has already been opened");
    }

    RowIterator inputIterator = input.open();
    activeIterator = new FilteringRowIterator(inputIterator);
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

  private final class FilteringRowIterator implements RowIterator {

    private final RowIterator delegate;

    private RelationalRow nextRow;
    private boolean nextRowReady;
    private boolean iteratorClosed;

    private FilteringRowIterator(RowIterator delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      if (iteratorClosed) {
        return false;
      }

      if (nextRowReady) {
        return true;
      }

      while (delegate.hasNext()) {
        RelationalRow candidate = delegate.next();

        if (predicate.test(candidate)) {
          nextRow = candidate;
          nextRowReady = true;
          return true;
        }
      }

      close();
      return false;
    }

    @Override
    public RelationalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more matching rows available");
      }

      RelationalRow result = nextRow;
      nextRow = null;
      nextRowReady = false;

      return result;
    }

    @Override
    public void close() {
      if (iteratorClosed) {
        return;
      }

      iteratorClosed = true;
      nextRow = null;
      nextRowReady = false;
      delegate.close();
    }
  }
}
