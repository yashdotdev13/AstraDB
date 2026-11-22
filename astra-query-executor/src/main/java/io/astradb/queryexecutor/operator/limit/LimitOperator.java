package io.astradb.queryexecutor.operator.limit;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Physical operator that limits the number of rows produced by its input.
 *
 * <p>Rows are consumed lazily. The input iterator is closed as soon as the requested number of rows
 * has been produced.
 */
public final class LimitOperator implements PhysicalOperator {

  private final PhysicalOperator input;
  private final long limit;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  public LimitOperator(PhysicalOperator input, long limit) {
    this.input = Objects.requireNonNull(input, "Input operator must not be null");

    if (limit < 0) {
      throw new IllegalArgumentException("Limit must not be negative");
    }

    this.limit = limit;
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Limit operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Limit operator has already been opened");
    }

    activeIterator = new LimitingRowIterator(input.open());
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

  private final class LimitingRowIterator implements RowIterator {

    private final RowIterator delegate;

    private long remaining;
    private boolean iteratorClosed;

    private LimitingRowIterator(RowIterator delegate) {
      this.delegate = delegate;
      this.remaining = limit;
    }

    @Override
    public boolean hasNext() {
      if (iteratorClosed || remaining == 0) {
        close();
        return false;
      }

      if (!delegate.hasNext()) {
        close();
        return false;
      }

      return true;
    }

    @Override
    public RelationalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more rows available");
      }

      RelationalRow row = delegate.next();
      remaining--;

      if (remaining == 0) {
        close();
      }

      return row;
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
