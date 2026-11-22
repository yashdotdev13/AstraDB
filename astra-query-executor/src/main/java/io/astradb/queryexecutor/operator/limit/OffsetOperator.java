package io.astradb.queryexecutor.operator.limit;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Physical operator that skips a specified number of rows from its input.
 *
 * <p>Rows are skipped lazily without materializing the input.
 */
public final class OffsetOperator implements PhysicalOperator {

  private final PhysicalOperator input;
  private final long offset;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  public OffsetOperator(PhysicalOperator input, long offset) {
    this.input = Objects.requireNonNull(input, "Input operator must not be null");

    if (offset < 0) {
      throw new IllegalArgumentException("Offset must not be negative");
    }

    this.offset = offset;
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Offset operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Offset operator has already been opened");
    }

    activeIterator = new OffsetRowIterator(input.open());
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

  private final class OffsetRowIterator implements RowIterator {

    private final RowIterator delegate;

    private long remainingOffset;
    private boolean iteratorClosed;
    private boolean offsetApplied;

    private OffsetRowIterator(RowIterator delegate) {
      this.delegate = Objects.requireNonNull(delegate, "Input iterator must not be null");
      this.remainingOffset = offset;
    }

    @Override
    public boolean hasNext() {
      if (iteratorClosed) {
        return false;
      }

      applyOffset();

      if (iteratorClosed) {
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

      return delegate.next();
    }

    private void applyOffset() {
      if (offsetApplied) {
        return;
      }

      while (remainingOffset > 0 && delegate.hasNext()) {
        delegate.next();
        remainingOffset--;
      }

      offsetApplied = true;

      if (remainingOffset > 0) {
        close();
      }
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
