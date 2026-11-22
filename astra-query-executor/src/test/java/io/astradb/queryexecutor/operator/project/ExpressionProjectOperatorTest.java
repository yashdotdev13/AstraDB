package io.astradb.queryexecutor.operator.project;

import static org.junit.jupiter.api.Assertions.*;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExpressionProjectOperatorTest {

  @Test
  void shouldEvaluateMultipleProjectionExpressions() {
    PhysicalOperator input = new TestOperator(List.of(RelationalRow.of(10, "Alice")));

    ExpressionProjectOperator operator =
        new ExpressionProjectOperator(
            input,
            List.of(
                row -> (Integer) row.getValue(0) + 10,
                row -> (Integer) row.getValue(0) * 2,
                row -> row.getValue(1)));

    RowIterator iterator = operator.open();

    assertTrue(iterator.hasNext());

    RelationalRow result = iterator.next();

    assertEquals(3, result.size());
    assertEquals(20, result.getValue(0));
    assertEquals(20, result.getValue(1));
    assertEquals("Alice", result.getValue(2));

    assertFalse(iterator.hasNext());
    operator.close();
  }

  @Test
  void shouldSupportNullProjectionResults() {
    PhysicalOperator input = new TestOperator(List.of(RelationalRow.of(10)));

    ExpressionProjectOperator operator = new ExpressionProjectOperator(input, List.of(row -> null));

    RelationalRow result = operator.open().next();

    assertNull(result.getValue(0));

    operator.close();
  }

  @Test
  void shouldReturnEmptyWhenInputIsEmpty() {
    PhysicalOperator input = new TestOperator(List.of());

    ExpressionProjectOperator operator =
        new ExpressionProjectOperator(input, List.of(row -> row.getValue(0)));

    RowIterator iterator = operator.open();

    assertFalse(iterator.hasNext());

    operator.close();
  }

  @Test
  void shouldRejectEmptyProjections() {
    PhysicalOperator input = new TestOperator(List.of());

    assertThrows(
        IllegalArgumentException.class, () -> new ExpressionProjectOperator(input, List.of()));
  }

  @Test
  void shouldRejectOpeningTwice() {
    PhysicalOperator input = new TestOperator(List.of());

    ExpressionProjectOperator operator =
        new ExpressionProjectOperator(input, List.of(row -> row.getValue(0)));

    operator.open();

    assertThrows(IllegalStateException.class, operator::open);

    operator.close();
  }

  @Test
  void shouldRejectOpeningAfterClose() {
    PhysicalOperator input = new TestOperator(List.of());

    ExpressionProjectOperator operator =
        new ExpressionProjectOperator(input, List.of(row -> row.getValue(0)));

    operator.close();

    assertThrows(IllegalStateException.class, operator::open);
  }

  private static final class TestOperator implements PhysicalOperator {

    private final List<RelationalRow> rows;
    private boolean closed;

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
        public void close() {
          // No resources to release in this test iterator.
        }
      };
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
