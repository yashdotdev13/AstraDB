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

class LimitOperatorTest {

  @Test
  void shouldReturnOnlyRequestedNumberOfRows() {
    LimitOperator operator =
        new LimitOperator(
            new InMemoryOperator(
                List.of(
                    RelationalRow.of(1, "Alice"),
                    RelationalRow.of(2, "Bob"),
                    RelationalRow.of(3, "Charlie"))),
            2);

    assertEquals(
        List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob")), collectRows(operator));
  }

  @Test
  void shouldReturnAllRowsWhenLimitExceedsInputSize() {
    List<RelationalRow> rows = List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    LimitOperator operator = new LimitOperator(new InMemoryOperator(rows), 10);

    assertEquals(rows, collectRows(operator));
  }

  @Test
  void shouldReturnEmptyResultWhenLimitIsZero() {
    InMemoryOperator input = new InMemoryOperator(List.of(RelationalRow.of(1, "Alice")));

    LimitOperator operator = new LimitOperator(input, 0);

    assertTrue(collectRows(operator).isEmpty());
    assertTrue(input.iteratorClosed);
  }

  @Test
  void shouldReturnEmptyResultForEmptyInput() {
    LimitOperator operator = new LimitOperator(new InMemoryOperator(List.of()), 5);

    assertTrue(collectRows(operator).isEmpty());
  }

  @Test
  void shouldRejectNegativeLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LimitOperator(new InMemoryOperator(List.of()), -1));
  }

  @Test
  void shouldCloseInputIteratorAfterReachingLimit() {
    InMemoryOperator input =
        new InMemoryOperator(
            List.of(
                RelationalRow.of(1, "Alice"),
                RelationalRow.of(2, "Bob"),
                RelationalRow.of(3, "Charlie")));

    LimitOperator operator = new LimitOperator(input, 2);

    try (RowIterator iterator = operator.open()) {
      assertEquals(RelationalRow.of(1, "Alice"), iterator.next());
      assertEquals(RelationalRow.of(2, "Bob"), iterator.next());

      assertTrue(input.iteratorClosed);
      assertFalse(iterator.hasNext());
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldSupportRepeatedHasNextCalls() {
    LimitOperator operator =
        new LimitOperator(
            new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"))),
            2);

    try (RowIterator iterator = operator.open()) {
      assertTrue(iterator.hasNext());
      assertTrue(iterator.hasNext());

      assertEquals(RelationalRow.of(1, "Alice"), iterator.next());
      assertEquals(RelationalRow.of(2, "Bob"), iterator.next());

      assertFalse(iterator.hasNext());
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldThrowWhenLimitIsExhausted() {
    LimitOperator operator =
        new LimitOperator(new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"))), 1);

    try (RowIterator iterator = operator.open()) {
      assertEquals(RelationalRow.of(1, "Alice"), iterator.next());

      assertThrows(NoSuchElementException.class, iterator::next);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldCloseInputIteratorWhenLimitIteratorCloses() {
    InMemoryOperator input = new InMemoryOperator(List.of(RelationalRow.of(1, "Alice")));

    LimitOperator operator = new LimitOperator(input, 5);
    RowIterator iterator = operator.open();

    iterator.close();

    assertTrue(input.iteratorClosed);
    assertFalse(iterator.hasNext());

    operator.close();
  }

  @Test
  void shouldRejectOpeningMoreThanOnce() {
    LimitOperator operator = new LimitOperator(new InMemoryOperator(List.of()), 5);

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  @Test
  void shouldRejectOpeningAfterClose() {
    LimitOperator operator = new LimitOperator(new InMemoryOperator(List.of()), 5);

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  private List<RelationalRow> collectRows(LimitOperator operator) {
    List<RelationalRow> result = new ArrayList<>();

    try (RowIterator iterator = operator.open()) {
      while (iterator.hasNext()) {
        result.add(iterator.next());
      }
    } finally {
      operator.close();
    }

    return List.copyOf(result);
  }

  private static final class InMemoryOperator implements PhysicalOperator {

    private final List<RelationalRow> rows;

    private boolean opened;
    private boolean closed;
    private boolean iteratorClosed;

    private InMemoryOperator(List<RelationalRow> rows) {
      this.rows = List.copyOf(rows);
    }

    @Override
    public RowIterator open() {
      if (closed || opened) {
        throw new IllegalStateException("Input operator cannot be opened");
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
      iteratorClosed = true;
    }
  }
}
