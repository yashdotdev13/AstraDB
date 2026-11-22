package io.astradb.queryexecutor.operator.aggregate;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class AggregateOperatorTest {

  @Test
  void shouldPerformGlobalAggregation() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(
                List.of(
                    RelationalRow.of(10, "Engineering"),
                    RelationalRow.of(20, "Engineering"),
                    RelationalRow.of(30, "Sales"))),
            new int[] {},
            List.of(
                AggregateCall.countStar(),
                AggregateCall.sum(0),
                AggregateCall.avg(0),
                AggregateCall.min(0),
                AggregateCall.max(0)));

    RelationalRow result = operator.open().next();

    assertEquals(5, result.size());
    assertEquals(3L, result.getValue(0));
    assertEquals(new BigDecimal("60"), result.getValue(1));
    assertEquals(new BigDecimal("20"), result.getValue(2));
    assertEquals(10, result.getValue(3));
    assertEquals(30, result.getValue(4));

    operator.close();
  }

  @Test
  void shouldGroupRowsByColumn() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(
                List.of(
                    RelationalRow.of(10, "Engineering"),
                    RelationalRow.of(20, "Engineering"),
                    RelationalRow.of(30, "Sales"))),
            new int[] {1},
            List.of(AggregateCall.countStar(), AggregateCall.sum(0)));

    List<RelationalRow> results = collect(operator);

    assertEquals(2, results.size());

    assertEquals("Engineering", results.get(0).getValue(0));
    assertEquals(2L, results.get(0).getValue(1));
    assertEquals(new BigDecimal("30"), results.get(0).getValue(2));

    assertEquals("Sales", results.get(1).getValue(0));
    assertEquals(1L, results.get(1).getValue(1));
    assertEquals(new BigDecimal("30"), results.get(1).getValue(2));
  }

  @Test
  void shouldCountOnlyNonNullColumnValues() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(
                List.of(
                    RelationalRow.of(10), RelationalRow.of((Object) null), RelationalRow.of(20))),
            new int[] {},
            List.of(AggregateCall.countStar(), AggregateCall.count(0)));

    RelationalRow result = operator.open().next();

    assertEquals(3L, result.getValue(0));
    assertEquals(2L, result.getValue(1));

    operator.close();
  }

  @Test
  void shouldReturnNullForEmptyGlobalAggregatesExceptCount() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(List.of()),
            new int[] {},
            List.of(
                AggregateCall.countStar(),
                AggregateCall.sum(0),
                AggregateCall.avg(0),
                AggregateCall.min(0),
                AggregateCall.max(0)));

    RelationalRow result = operator.open().next();

    assertEquals(0L, result.getValue(0));
    assertNull(result.getValue(1));
    assertNull(result.getValue(2));
    assertNull(result.getValue(3));
    assertNull(result.getValue(4));

    operator.close();
  }

  @Test
  void shouldReturnNoRowsForEmptyGroupedAggregation() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(List.of()), new int[] {0}, List.of(AggregateCall.countStar()));

    RowIterator iterator = operator.open();

    assertFalse(iterator.hasNext());

    operator.close();
  }

  @Test
  void shouldRejectNegativeGroupColumnIndex() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AggregateOperator(
                new TestOperator(List.of()), new int[] {-1}, List.of(AggregateCall.countStar())));
  }

  @Test
  void shouldRejectEmptyAggregateCalls() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AggregateOperator(new TestOperator(List.of()), new int[] {}, List.of()));
  }

  @Test
  void shouldGroupRowsByMultipleColumns() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(
                List.of(
                    RelationalRow.of(10, "Engineering", "US"),
                    RelationalRow.of(20, "Engineering", "US"),
                    RelationalRow.of(30, "Engineering", "UK"),
                    RelationalRow.of(40, "Sales", "US"))),
            new int[] {1, 2},
            List.of(AggregateCall.countStar(), AggregateCall.sum(0)));

    List<RelationalRow> results = collect(operator);

    assertEquals(3, results.size());

    assertEquals("Engineering", results.get(0).getValue(0));
    assertEquals("US", results.get(0).getValue(1));
    assertEquals(2L, results.get(0).getValue(2));
    assertEquals(new BigDecimal("30"), results.get(0).getValue(3));

    assertEquals("Engineering", results.get(1).getValue(0));
    assertEquals("UK", results.get(1).getValue(1));
    assertEquals(1L, results.get(1).getValue(2));

    assertEquals("Sales", results.get(2).getValue(0));
    assertEquals("US", results.get(2).getValue(1));
    assertEquals(1L, results.get(2).getValue(2));
  }

  @Test
  void shouldGroupNullValuesTogether() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(
                List.of(
                    RelationalRow.of(10, (Object) null),
                    RelationalRow.of(20, (Object) null),
                    RelationalRow.of(30, "Sales"))),
            new int[] {1},
            List.of(AggregateCall.countStar(), AggregateCall.sum(0)));

    List<RelationalRow> results = collect(operator);

    assertEquals(2, results.size());

    assertNull(results.get(0).getValue(0));
    assertEquals(2L, results.get(0).getValue(1));
    assertEquals(new BigDecimal("30"), results.get(0).getValue(2));

    assertEquals("Sales", results.get(1).getValue(0));
    assertEquals(1L, results.get(1).getValue(1));
  }

  @Test
  void shouldRejectDuplicateGroupByColumns() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AggregateOperator(
                new TestOperator(List.of()), new int[] {0, 0}, List.of(AggregateCall.countStar())));
  }

  @Test
  void shouldRejectAggregateColumnOutsideRowBounds() {
    AggregateOperator operator =
        new AggregateOperator(
            new TestOperator(List.of(RelationalRow.of(10))),
            new int[] {},
            List.of(AggregateCall.sum(2)));

    assertThrows(IllegalArgumentException.class, operator::open);
  }

  private List<RelationalRow> collect(AggregateOperator operator) {
    RowIterator iterator = operator.open();

    try {
      java.util.ArrayList<RelationalRow> rows = new java.util.ArrayList<>();

      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }

      return List.copyOf(rows);
    } finally {
      iterator.close();
      operator.close();
    }
  }

  private static final class TestOperator implements PhysicalOperator {

    private final List<RelationalRow> rows;

    private TestOperator(List<RelationalRow> rows) {
      this.rows = rows;
    }

    @Override
    public RowIterator open() {
      Iterator<RelationalRow> delegate = rows.iterator();

      return new RowIterator() {
        @Override
        public boolean hasNext() {
          return delegate.hasNext();
        }

        @Override
        public RelationalRow next() {
          return delegate.next();
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public void close() {}
  }
}
