package io.astradb.queryexecutor.operator.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class HashJoinOperatorTest {

  // ---------------------------------------------------------------------------
  // Single-column INNER HASH JOIN tests
  // ---------------------------------------------------------------------------

  @Test
  void shouldReturnMatchingRows() {
    var left = rows(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    var right = rows(RelationalRow.of(1, "Order-A"), RelationalRow.of(2, "Order-B"));

    List<RelationalRow> result = execute(left, right);

    assertEquals(
        List.of(
            RelationalRow.of(1, "Alice", 1, "Order-A"), RelationalRow.of(2, "Bob", 2, "Order-B")),
        result);
  }

  @Test
  void shouldReturnMultipleMatchesForDuplicateKeys() {
    var left = rows(RelationalRow.of(1, "Alice"));

    var right =
        rows(
            RelationalRow.of(1, "Order-A"),
            RelationalRow.of(1, "Order-B"),
            RelationalRow.of(2, "Order-C"));

    List<RelationalRow> result = execute(left, right);

    assertEquals(
        List.of(
            RelationalRow.of(1, "Alice", 1, "Order-A"), RelationalRow.of(1, "Alice", 1, "Order-B")),
        result);
  }

  @Test
  void shouldReturnEmptyResultWhenThereAreNoMatches() {
    var left = rows(RelationalRow.of(1, "Alice"));
    var right = rows(RelationalRow.of(99, "Order-A"));

    assertEquals(List.of(), execute(left, right));
  }

  @Test
  void shouldReturnEmptyResultWhenLeftInputIsEmpty() {
    var left = rows();
    var right = rows(RelationalRow.of(1, "Order-A"));

    assertEquals(List.of(), execute(left, right));
  }

  @Test
  void shouldReturnEmptyResultWhenRightInputIsEmpty() {
    var left = rows(RelationalRow.of(1, "Alice"));
    var right = rows();

    assertEquals(List.of(), execute(left, right));
  }

  @Test
  void shouldNotMatchNullJoinKeys() {
    var left = rows(RelationalRow.of(null, "Alice"));
    var right = rows(RelationalRow.of(null, "Order-A"));

    assertEquals(List.of(), execute(left, right));
  }

  @Test
  void shouldMatchEquivalentNumericKeys() {
    var left = rows(RelationalRow.of(10, "Alice"));
    var right = rows(RelationalRow.of(new BigDecimal("10.0"), "Order-A"));

    List<RelationalRow> result = execute(left, right);

    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).getValue(1));
    assertEquals("Order-A", result.get(0).getValue(3));
  }

  @Test
  void shouldRejectOpeningOperatorMoreThanOnce() {
    HashJoinOperator operator =
        new HashJoinOperator(
            rows(RelationalRow.of(1)),
            rows(RelationalRow.of(1)),
            row -> row.getValue(0),
            row -> row.getValue(0));

    try {
      operator.open();

      assertThrows(IllegalStateException.class, operator::open);
    } finally {
      operator.close();
    }
  }

  @Test
  void shouldRejectOpeningClosedOperator() {
    HashJoinOperator operator =
        new HashJoinOperator(
            rows(RelationalRow.of(1)),
            rows(RelationalRow.of(1)),
            row -> row.getValue(0),
            row -> row.getValue(0));

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  // ---------------------------------------------------------------------------
  // Composite-key INNER HASH JOIN tests
  // ---------------------------------------------------------------------------

  @Test
  void shouldReturnMatchingRowsForCompositeKeys() {
    var left =
        rows(
            RelationalRow.of(1, "NORTH", "Alice"),
            RelationalRow.of(2, "SOUTH", "Bob"),
            RelationalRow.of(3, "EAST", "Charlie"));

    var right =
        rows(
            RelationalRow.of(1, "NORTH", "Order-A"),
            RelationalRow.of(2, "SOUTH", "Order-B"),
            RelationalRow.of(1, "SOUTH", "Order-C"));

    List<RelationalRow> result =
        execute(
            left,
            right,
            List.of(row -> row.getValue(0), row -> row.getValue(1)),
            List.of(row -> row.getValue(0), row -> row.getValue(1)));

    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).getValue(2));
    assertEquals("Order-A", result.get(0).getValue(5));
    assertEquals("Bob", result.get(1).getValue(2));
    assertEquals("Order-B", result.get(1).getValue(5));
  }

  @Test
  void shouldNotMatchWhenOnlyOneCompositeKeyComponentMatches() {
    var left = rows(RelationalRow.of(1, "NORTH", "Alice"));

    var right =
        rows(RelationalRow.of(1, "SOUTH", "Order-A"), RelationalRow.of(2, "NORTH", "Order-B"));

    List<RelationalRow> result =
        execute(
            left,
            right,
            List.of(row -> row.getValue(0), row -> row.getValue(1)),
            List.of(row -> row.getValue(0), row -> row.getValue(1)));

    assertEquals(List.of(), result);
  }

  @Test
  void shouldReturnMultipleMatchesForDuplicateCompositeKeys() {
    var left = rows(RelationalRow.of(1, "NORTH", "Alice"));

    var right =
        rows(
            RelationalRow.of(1, "NORTH", "Order-A"),
            RelationalRow.of(1, "NORTH", "Order-B"),
            RelationalRow.of(1, "SOUTH", "Order-C"));

    List<RelationalRow> result =
        execute(
            left,
            right,
            List.of(row -> row.getValue(0), row -> row.getValue(1)),
            List.of(row -> row.getValue(0), row -> row.getValue(1)));

    assertEquals(2, result.size());
    assertEquals("Order-A", result.get(0).getValue(5));
    assertEquals("Order-B", result.get(1).getValue(5));
  }

  @Test
  void shouldNotMatchWhenFirstCompositeKeyComponentIsNull() {
    var left = rows(RelationalRow.of(null, "NORTH", "Alice"));
    var right = rows(RelationalRow.of(null, "NORTH", "Order-A"));

    List<RelationalRow> result =
        execute(
            left,
            right,
            List.of(row -> row.getValue(0), row -> row.getValue(1)),
            List.of(row -> row.getValue(0), row -> row.getValue(1)));

    assertEquals(List.of(), result);
  }

  @Test
  void shouldNotMatchWhenSecondCompositeKeyComponentIsNull() {
    var left = rows(RelationalRow.of(1, null, "Alice"));
    var right = rows(RelationalRow.of(1, null, "Order-A"));

    List<RelationalRow> result =
        execute(
            left,
            right,
            List.of(row -> row.getValue(0), row -> row.getValue(1)),
            List.of(row -> row.getValue(0), row -> row.getValue(1)));

    assertEquals(List.of(), result);
  }

  @Test
  void shouldNormalizeNumericValuesInCompositeKeys() {
    var left = rows(RelationalRow.of(10, new BigDecimal("20.0"), "Alice"));

    var right = rows(RelationalRow.of(new BigDecimal("10.0"), 20, "Order-A"));

    List<RelationalRow> result =
        execute(
            left,
            right,
            List.of(row -> row.getValue(0), row -> row.getValue(1)),
            List.of(row -> row.getValue(0), row -> row.getValue(1)));

    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).getValue(2));
    assertEquals("Order-A", result.get(0).getValue(5));
  }

  @Test
  void shouldRejectMismatchedCompositeKeyExtractorCounts() {
    PhysicalOperator left = rows(RelationalRow.of(1, "NORTH"));
    PhysicalOperator right = rows(RelationalRow.of(1, "NORTH"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HashJoinOperator(
                left,
                right,
                List.of(row -> row.getValue(0), row -> row.getValue(1)),
                List.of(row -> row.getValue(0))));
  }

  // ---------------------------------------------------------------------------
  // Execution helpers
  // ---------------------------------------------------------------------------

  private List<RelationalRow> execute(PhysicalOperator left, PhysicalOperator right) {
    return execute(left, right, List.of(row -> row.getValue(0)), List.of(row -> row.getValue(0)));
  }

  private List<RelationalRow> execute(
      PhysicalOperator left,
      PhysicalOperator right,
      List<Function<RelationalRow, Object>> leftKeyExtractors,
      List<Function<RelationalRow, Object>> rightKeyExtractors) {

    HashJoinOperator operator =
        new HashJoinOperator(left, right, leftKeyExtractors, rightKeyExtractors);

    List<RelationalRow> result = new ArrayList<>();

    try {
      RowIterator iterator = operator.open();

      while (iterator.hasNext()) {
        result.add(iterator.next());
      }

      iterator.close();
    } finally {
      operator.close();
    }

    return result;
  }

  private PhysicalOperator rows(RelationalRow... rows) {
    return new InMemoryOperator(List.of(rows));
  }

  // ---------------------------------------------------------------------------
  // In-memory physical operator used by tests
  // ---------------------------------------------------------------------------

  private static final class InMemoryOperator implements PhysicalOperator {

    private final List<RelationalRow> rows;
    private boolean opened;
    private boolean closed;

    private InMemoryOperator(List<RelationalRow> rows) {
      this.rows = rows;
    }

    @Override
    public RowIterator open() {
      if (closed || opened) {
        throw new IllegalStateException("Operator cannot be opened");
      }

      opened = true;
      Iterator<RelationalRow> iterator = rows.iterator();

      return new RowIterator() {

        private boolean iteratorClosed;

        @Override
        public boolean hasNext() {
          return !iteratorClosed && iterator.hasNext();
        }

        @Override
        public RelationalRow next() {
          if (iteratorClosed || !iterator.hasNext()) {
            throw new java.util.NoSuchElementException();
          }

          return iterator.next();
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
