package io.astradb.queryexecutor.operator.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class NestedLoopJoinOperatorTest {

  @Test
  void shouldReturnMatchingRows() {
    PhysicalOperator left = operator(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    PhysicalOperator right =
        operator(RelationalRow.of(1, "Engineering"), RelationalRow.of(2, "Sales"));

    NestedLoopJoinOperator join =
        new NestedLoopJoinOperator(left, right, row -> row.getValue(0).equals(row.getValue(2)));

    try {
      List<RelationalRow> result = collect(join);

      assertEquals(
          List.of(
              RelationalRow.of(1, "Alice", 1, "Engineering"),
              RelationalRow.of(2, "Bob", 2, "Sales")),
          result);
    } finally {
      join.close();
    }
  }

  @Test
  void shouldReturnMultipleMatchesForOneLeftRow() {
    PhysicalOperator left = operator(RelationalRow.of(1, "Alice"));

    PhysicalOperator right =
        operator(
            RelationalRow.of(1, "Order-101"),
            RelationalRow.of(1, "Order-102"),
            RelationalRow.of(2, "Order-103"));

    NestedLoopJoinOperator join =
        new NestedLoopJoinOperator(left, right, row -> row.getValue(0).equals(row.getValue(2)));

    try {
      List<RelationalRow> result = collect(join);

      assertEquals(
          List.of(
              RelationalRow.of(1, "Alice", 1, "Order-101"),
              RelationalRow.of(1, "Alice", 1, "Order-102")),
          result);
    } finally {
      join.close();
    }
  }

  @Test
  void shouldReturnEmptyResultWhenThereAreNoMatches() {
    PhysicalOperator left = operator(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob"));

    PhysicalOperator right = operator(RelationalRow.of(3, "Order-101"));

    NestedLoopJoinOperator join =
        new NestedLoopJoinOperator(left, right, row -> row.getValue(0).equals(row.getValue(2)));

    try {
      assertTrue(collect(join).isEmpty());
    } finally {
      join.close();
    }
  }

  @Test
  void shouldReturnEmptyResultWhenLeftInputIsEmpty() {
    PhysicalOperator left = operator();

    PhysicalOperator right = operator(RelationalRow.of(1, "Order-101"));

    NestedLoopJoinOperator join =
        new NestedLoopJoinOperator(left, right, row -> row.getValue(0).equals(row.getValue(2)));

    try {
      assertTrue(collect(join).isEmpty());
    } finally {
      join.close();
    }
  }

  @Test
  void shouldReturnEmptyResultWhenRightInputIsEmpty() {
    PhysicalOperator left = operator(RelationalRow.of(1, "Alice"));

    PhysicalOperator right = operator();

    NestedLoopJoinOperator join =
        new NestedLoopJoinOperator(left, right, row -> row.getValue(0).equals(row.getValue(2)));

    try {
      assertTrue(collect(join).isEmpty());
    } finally {
      join.close();
    }
  }

  @Test
  void shouldNotReturnRowsWhenJoinConditionIsFalse() {
    PhysicalOperator left = operator(RelationalRow.of(1, "Alice"));

    PhysicalOperator right = operator(RelationalRow.of(1, "Order-101"));

    NestedLoopJoinOperator join = new NestedLoopJoinOperator(left, right, row -> false);

    try {
      assertFalse(collect(join).iterator().hasNext());
    } finally {
      join.close();
    }
  }

  private List<RelationalRow> collect(PhysicalOperator operator) {
    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = operator.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }

    return rows;
  }

  private PhysicalOperator operator(RelationalRow... rows) {
    List<RelationalRow> data = List.of(rows);

    return new PhysicalOperator() {

      private boolean closed;

      @Override
      public RowIterator open() {
        return new RowIterator() {

          private final java.util.Iterator<RelationalRow> iterator = data.iterator();
          private boolean iteratorClosed;

          @Override
          public boolean hasNext() {
            return !iteratorClosed && iterator.hasNext();
          }

          @Override
          public RelationalRow next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
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
    };
  }
}
