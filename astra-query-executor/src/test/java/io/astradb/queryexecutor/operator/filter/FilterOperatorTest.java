package io.astradb.queryexecutor.operator.filter;

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

class FilterOperatorTest {

  @Test
  void shouldReturnOnlyRowsMatchingPredicate() {
    List<RelationalRow> rows =
        List.of(
            RelationalRow.of(1, "Alice"),
            RelationalRow.of(2, "Bob"),
            RelationalRow.of(3, "Charlie"));

    FilterOperator operator =
        new FilterOperator(new InMemoryOperator(rows), row -> (Integer) row.getValue(0) > 1);

    assertEquals(
        List.of(RelationalRow.of(2, "Bob"), RelationalRow.of(3, "Charlie")), collectRows(operator));
  }

  @Test
  void shouldReturnEmptyResultWhenNoRowsMatch() {
    FilterOperator operator =
        new FilterOperator(
            new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"))), row -> false);

    assertTrue(collectRows(operator).isEmpty());
  }

  @Test
  void shouldReturnAllRowsWhenEveryRowMatches() {
    List<RelationalRow> rows = List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    FilterOperator operator = new FilterOperator(new InMemoryOperator(rows), row -> true);

    assertEquals(rows, collectRows(operator));
  }

  @Test
  void shouldSupportRepeatedHasNextCalls() {
    FilterOperator operator =
        new FilterOperator(
            new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"))),
            row -> true);

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
  void shouldThrowWhenNoMatchingRowsRemain() {
    FilterOperator operator = new FilterOperator(new InMemoryOperator(List.of()), row -> true);

    try (RowIterator iterator = operator.open()) {
      assertThrows(NoSuchElementException.class, iterator::next);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldCloseInputIteratorWhenFilterIteratorCloses() {
    InMemoryOperator input = new InMemoryOperator(List.of(RelationalRow.of(1, "Alice")));

    FilterOperator operator = new FilterOperator(input, row -> true);
    RowIterator iterator = operator.open();

    iterator.close();

    assertFalse(iterator.hasNext());
    assertTrue(input.iteratorClosed);

    operator.close();
  }

  @Test
  void shouldRejectOpeningMoreThanOnce() {
    FilterOperator operator = new FilterOperator(new InMemoryOperator(List.of()), row -> true);

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  @Test
  void shouldRejectOpeningAfterClose() {
    FilterOperator operator = new FilterOperator(new InMemoryOperator(List.of()), row -> true);

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  private List<RelationalRow> collectRows(FilterOperator operator) {
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
