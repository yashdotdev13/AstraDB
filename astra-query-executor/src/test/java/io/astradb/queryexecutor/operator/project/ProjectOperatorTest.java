package io.astradb.queryexecutor.operator.project;

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

class ProjectOperatorTest {

  @Test
  void shouldProjectSelectedColumns() {
    ProjectOperator operator =
        new ProjectOperator(
            new InMemoryOperator(List.of(RelationalRow.of(1, "Alice", "alice@example.com"))), 0, 2);

    assertEquals(List.of(RelationalRow.of(1, "alice@example.com")), collectRows(operator));
  }

  @Test
  void shouldPreserveRequestedColumnOrder() {
    ProjectOperator operator =
        new ProjectOperator(
            new InMemoryOperator(List.of(RelationalRow.of(1, "Alice", "alice@example.com"))),
            2,
            0,
            1);

    assertEquals(List.of(RelationalRow.of("alice@example.com", 1, "Alice")), collectRows(operator));
  }

  @Test
  void shouldAllowRepeatedColumnIndexes() {
    ProjectOperator operator =
        new ProjectOperator(new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"))), 1, 1, 0);

    assertEquals(List.of(RelationalRow.of("Alice", "Alice", 1)), collectRows(operator));
  }

  @Test
  void shouldProjectMultipleRows() {
    List<RelationalRow> rows =
        List.of(RelationalRow.of(1, "Alice", true), RelationalRow.of(2, "Bob", false));

    ProjectOperator operator = new ProjectOperator(new InMemoryOperator(rows), 1, 2);

    assertEquals(
        List.of(RelationalRow.of("Alice", true), RelationalRow.of("Bob", false)),
        collectRows(operator));
  }

  @Test
  void shouldReturnEmptyResultForEmptyInput() {
    ProjectOperator operator = new ProjectOperator(new InMemoryOperator(List.of()), 0);

    assertTrue(collectRows(operator).isEmpty());
  }

  @Test
  void shouldRejectNegativeColumnIndex() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ProjectOperator(new InMemoryOperator(List.of()), -1));
  }

  @Test
  void shouldRejectEmptyColumnIndexes() {
    assertThrows(
        IllegalArgumentException.class, () -> new ProjectOperator(new InMemoryOperator(List.of())));
  }

  @Test
  void shouldRejectColumnIndexOutsideRowSize() {
    ProjectOperator operator =
        new ProjectOperator(new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"))), 2);

    try (RowIterator iterator = operator.open()) {
      assertThrows(IllegalArgumentException.class, iterator::next);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldSupportRepeatedHasNextCalls() {
    ProjectOperator operator =
        new ProjectOperator(new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"))), 1);

    try (RowIterator iterator = operator.open()) {
      assertTrue(iterator.hasNext());
      assertTrue(iterator.hasNext());
      assertEquals(RelationalRow.of("Alice"), iterator.next());
      assertFalse(iterator.hasNext());
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldThrowWhenNoRowsRemain() {
    ProjectOperator operator = new ProjectOperator(new InMemoryOperator(List.of()), 0);

    try (RowIterator iterator = operator.open()) {
      assertThrows(NoSuchElementException.class, iterator::next);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldCloseInputIteratorWhenProjectIteratorCloses() {
    InMemoryOperator input = new InMemoryOperator(List.of(RelationalRow.of(1, "Alice")));

    ProjectOperator operator = new ProjectOperator(input, 0);
    RowIterator iterator = operator.open();

    iterator.close();

    assertFalse(iterator.hasNext());
    assertTrue(input.iteratorClosed);

    operator.close();
  }

  @Test
  void shouldRejectOpeningMoreThanOnce() {
    ProjectOperator operator = new ProjectOperator(new InMemoryOperator(List.of()), 0);

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  @Test
  void shouldRejectOpeningAfterClose() {
    ProjectOperator operator = new ProjectOperator(new InMemoryOperator(List.of()), 0);

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  private List<RelationalRow> collectRows(ProjectOperator operator) {
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
