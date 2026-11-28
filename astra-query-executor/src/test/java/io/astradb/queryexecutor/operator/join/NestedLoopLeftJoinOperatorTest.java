package io.astradb.queryexecutor.operator.join;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class NestedLoopLeftJoinOperatorTest {

  @Test
  void shouldPreserveUnmatchedLeftRowsWithNullValues() {
    List<RelationalRow> leftRows =
        List.of(
            RelationalRow.of(1, "Alice"),
            RelationalRow.of(2, "Bob"),
            RelationalRow.of(3, "Charlie"));

    List<RelationalRow> rightRows =
        List.of(RelationalRow.of(101, 1, 500), RelationalRow.of(102, 2, 300));

    NestedLoopJoinOperator operator = createLeftJoin(leftRows, rightRows, 3);

    List<RelationalRow> result = collectRows(operator);

    assertEquals(
        List.of(
            RelationalRow.of(1, "Alice", 101, 1, 500),
            RelationalRow.of(2, "Bob", 102, 2, 300),
            RelationalRow.of(3, "Charlie", null, null, null)),
        result);
  }

  @Test
  void shouldReturnAllMatchingRowsAndPreserveUnmatchedLeftRows() {
    List<RelationalRow> leftRows =
        List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    List<RelationalRow> rightRows =
        List.of(RelationalRow.of(101, 1), RelationalRow.of(102, 1), RelationalRow.of(103, 2));

    NestedLoopJoinOperator operator = createLeftJoin(leftRows, rightRows, 2);

    List<RelationalRow> result = collectRows(operator);

    assertEquals(
        List.of(
            RelationalRow.of(1, "Alice", 101, 1),
            RelationalRow.of(1, "Alice", 102, 1),
            RelationalRow.of(2, "Bob", 103, 2)),
        result);
  }

  @Test
  void shouldPreserveAllLeftRowsWhenRightInputIsEmpty() {
    List<RelationalRow> leftRows =
        List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    NestedLoopJoinOperator operator = createLeftJoin(leftRows, List.of(), 3);

    List<RelationalRow> result = collectRows(operator);

    assertEquals(
        List.of(
            RelationalRow.of(1, "Alice", null, null, null),
            RelationalRow.of(2, "Bob", null, null, null)),
        result);
  }

  @Test
  void shouldReturnEmptyResultWhenLeftInputIsEmpty() {
    NestedLoopJoinOperator operator =
        createLeftJoin(List.of(), List.of(RelationalRow.of(101, 1)), 2);

    assertEquals(List.of(), collectRows(operator));
  }

  private NestedLoopJoinOperator createLeftJoin(
      List<RelationalRow> leftRows, List<RelationalRow> rightRows, int rightColumnCount) {

    return new NestedLoopJoinOperator(
        new InMemoryOperator(leftRows),
        new InMemoryOperator(rightRows),
        row -> row.getValue(0).equals(row.getValue(3)),
        true,
        rightColumnCount);
  }

  private List<RelationalRow> collectRows(PhysicalOperator operator) {
    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = operator.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    } finally {
      operator.close();
    }

    return rows;
  }

  private static final class InMemoryOperator implements PhysicalOperator {

    private final List<RelationalRow> rows;
    private boolean closed;

    private InMemoryOperator(List<RelationalRow> rows) {
      this.rows = List.copyOf(rows);
    }

    @Override
    public RowIterator open() {
      if (closed) {
        throw new IllegalStateException("Operator is closed");
      }

      Iterator<RelationalRow> delegate = rows.iterator();

      return new RowIterator() {
        private boolean iteratorClosed;

        @Override
        public boolean hasNext() {
          return !iteratorClosed && delegate.hasNext();
        }

        @Override
        public RelationalRow next() {
          if (!hasNext()) {
            throw new NoSuchElementException("No more rows available");
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
    }
  }
}
