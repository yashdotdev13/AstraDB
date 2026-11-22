package io.astradb.queryexecutor.operator.sort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class SortOperatorTest {

  @Test
  void shouldSortRowsInAscendingOrder() {
    SortOperator operator =
        new SortOperator(
            source(RelationalRow.of(30), RelationalRow.of(10), RelationalRow.of(20)),
            List.of(key(0, SortKey.Direction.ASCENDING)));

    assertEquals(
        List.of(RelationalRow.of(10), RelationalRow.of(20), RelationalRow.of(30)),
        collect(operator));
  }

  @Test
  void shouldSortRowsInDescendingOrder() {
    SortOperator operator =
        new SortOperator(
            source(RelationalRow.of(30), RelationalRow.of(10), RelationalRow.of(20)),
            List.of(key(0, SortKey.Direction.DESCENDING)));

    assertEquals(
        List.of(RelationalRow.of(30), RelationalRow.of(20), RelationalRow.of(10)),
        collect(operator));
  }

  @Test
  void shouldSortUsingMultipleColumns() {
    SortOperator operator =
        new SortOperator(
            source(
                RelationalRow.of("Engineering", 10),
                RelationalRow.of("Engineering", 30),
                RelationalRow.of("Sales", 20),
                RelationalRow.of("Engineering", 20)),
            List.of(key(0, SortKey.Direction.ASCENDING), key(1, SortKey.Direction.DESCENDING)));

    assertEquals(
        List.of(
            RelationalRow.of("Engineering", 30),
            RelationalRow.of("Engineering", 20),
            RelationalRow.of("Engineering", 10),
            RelationalRow.of("Sales", 20)),
        collect(operator));
  }

  @Test
  void shouldPlaceNullValuesFirst() {
    SortOperator operator =
        new SortOperator(
            source(RelationalRow.of(20), RelationalRow.of((Object) null), RelationalRow.of(10)),
            List.of(new SortKey(0, SortKey.Direction.ASCENDING, SortKey.NullDirection.FIRST)));

    assertEquals(
        List.of(RelationalRow.of((Object) null), RelationalRow.of(10), RelationalRow.of(20)),
        collect(operator));
  }

  @Test
  void shouldPlaceNullValuesLast() {
    SortOperator operator =
        new SortOperator(
            source(RelationalRow.of(20), RelationalRow.of((Object) null), RelationalRow.of(10)),
            List.of(new SortKey(0, SortKey.Direction.ASCENDING, SortKey.NullDirection.LAST)));

    assertEquals(
        List.of(RelationalRow.of(10), RelationalRow.of(20), RelationalRow.of((Object) null)),
        collect(operator));
  }

  @Test
  void shouldPreserveInputOrderForEqualSortKeys() {
    SortOperator operator =
        new SortOperator(
            source(
                RelationalRow.of(10, "Alice"),
                RelationalRow.of(10, "Bob"),
                RelationalRow.of(10, "Charlie")),
            List.of(key(0, SortKey.Direction.ASCENDING)));

    assertEquals(
        List.of(
            RelationalRow.of(10, "Alice"),
            RelationalRow.of(10, "Bob"),
            RelationalRow.of(10, "Charlie")),
        collect(operator));
  }

  @Test
  void shouldReturnEmptyResultForEmptyInput() {
    SortOperator operator =
        new SortOperator(source(), List.of(key(0, SortKey.Direction.ASCENDING)));

    assertEquals(List.of(), collect(operator));
  }

  @Test
  void shouldRejectEmptySortKeys() {
    assertThrows(IllegalArgumentException.class, () -> new SortOperator(source(), List.of()));
  }

  @Test
  void shouldRejectOpeningOperatorMoreThanOnce() {
    SortOperator operator =
        new SortOperator(
            source(RelationalRow.of(10)), List.of(key(0, SortKey.Direction.ASCENDING)));

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  private SortKey key(int index, SortKey.Direction direction) {
    return new SortKey(index, direction, SortKey.NullDirection.LAST);
  }

  private List<RelationalRow> collect(SortOperator operator) {
    try (RowIterator iterator = operator.open()) {
      List<RelationalRow> rows = new ArrayList<>();

      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }

      return List.copyOf(rows);
    } finally {
      operator.close();
    }
  }

  private PhysicalOperator source(RelationalRow... rows) {
    List<RelationalRow> inputRows = List.of(rows);

    return new PhysicalOperator() {

      private boolean opened;
      private boolean closed;

      @Override
      public RowIterator open() {
        if (closed || opened) {
          throw new IllegalStateException("Source cannot be opened");
        }

        opened = true;

        return new RowIterator() {
          private int index;
          private boolean iteratorClosed;

          @Override
          public boolean hasNext() {
            return !iteratorClosed && index < inputRows.size();
          }

          @Override
          public RelationalRow next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            return inputRows.get(index++);
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
