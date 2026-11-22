package io.astradb.queryexecutor.operator.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class OffsetOperatorTest {

  @Test
  void shouldSkipRequestedNumberOfRows() {
    OffsetOperator operator = new OffsetOperator(source(row(1), row(2), row(3), row(4), row(5)), 2);

    assertEquals(List.of(row(3), row(4), row(5)), collect(operator));
  }

  @Test
  void shouldReturnAllRowsWhenOffsetIsZero() {
    OffsetOperator operator = new OffsetOperator(source(row(1), row(2), row(3)), 0);

    assertEquals(List.of(row(1), row(2), row(3)), collect(operator));
  }

  @Test
  void shouldReturnEmptyWhenOffsetEqualsInputSize() {
    OffsetOperator operator = new OffsetOperator(source(row(1), row(2), row(3)), 3);

    assertTrue(collect(operator).isEmpty());
  }

  @Test
  void shouldReturnEmptyWhenOffsetExceedsInputSize() {
    OffsetOperator operator = new OffsetOperator(source(row(1), row(2)), 5);

    assertTrue(collect(operator).isEmpty());
  }

  @Test
  void shouldHandleEmptyInput() {
    OffsetOperator operator = new OffsetOperator(source(), 2);

    assertTrue(collect(operator).isEmpty());
  }

  @Test
  void shouldRejectNegativeOffset() {
    assertThrows(IllegalArgumentException.class, () -> new OffsetOperator(source(row(1)), -1));
  }

  @Test
  void shouldRejectNullInput() {
    assertThrows(NullPointerException.class, () -> new OffsetOperator(null, 1));
  }

  @Test
  void shouldRejectSecondOpen() {
    OffsetOperator operator = new OffsetOperator(source(row(1), row(2)), 1);

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  @Test
  void shouldRejectOpenAfterClose() {
    OffsetOperator operator = new OffsetOperator(source(row(1)), 0);

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  @Test
  void shouldCloseInputWhenOperatorIsClosed() {
    TrackingPhysicalOperator input = new TrackingPhysicalOperator(List.of(row(1), row(2)));

    OffsetOperator operator = new OffsetOperator(input, 1);

    RowIterator iterator = operator.open();

    operator.close();

    assertTrue(input.closed);
    assertFalse(iterator.hasNext());
  }

  @Test
  void shouldCloseInputIteratorAfterExhaustion() {
    TrackingPhysicalOperator input = new TrackingPhysicalOperator(List.of(row(1), row(2)));

    OffsetOperator operator = new OffsetOperator(input, 1);

    RowIterator iterator = operator.open();

    assertEquals(row(2), iterator.next());
    assertFalse(iterator.hasNext());
    assertTrue(input.iteratorClosed);

    operator.close();
  }

  @Test
  void shouldThrowWhenNextIsCalledAfterExhaustion() {
    OffsetOperator operator = new OffsetOperator(source(row(1)), 1);

    RowIterator iterator = operator.open();

    assertFalse(iterator.hasNext());

    assertThrows(NoSuchElementException.class, iterator::next);

    operator.close();
  }

  private static RelationalRow row(int value) {
    return RelationalRow.of(value);
  }

  private static List<RelationalRow> collect(PhysicalOperator operator) {
    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = operator.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    } finally {
      operator.close();
    }

    return List.copyOf(rows);
  }

  private static PhysicalOperator source(RelationalRow... rows) {
    return new TrackingPhysicalOperator(List.of(rows));
  }

  private static final class TrackingPhysicalOperator implements PhysicalOperator {

    private final List<RelationalRow> rows;

    private boolean opened;
    private boolean closed;
    private boolean iteratorClosed;

    private TrackingPhysicalOperator(List<RelationalRow> rows) {
      this.rows = rows;
    }

    @Override
    public RowIterator open() {
      if (closed || opened) {
        throw new IllegalStateException("Source cannot be opened");
      }

      opened = true;
      Iterator<RelationalRow> delegate = rows.iterator();

      return new RowIterator() {
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
      };
    }

    @Override
    public void close() {
      closed = true;

      if (opened) {
        iteratorClosed = true;
      }
    }
  }
}
