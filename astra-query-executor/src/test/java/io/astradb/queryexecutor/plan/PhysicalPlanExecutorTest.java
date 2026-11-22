package io.astradb.queryexecutor.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.queryexecutor.operator.filter.FilterOperator;
import io.astradb.queryexecutor.operator.limit.LimitOperator;
import io.astradb.queryexecutor.operator.project.ProjectOperator;
import io.astradb.sql.model.RelationalRow;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class PhysicalPlanExecutorTest {

  private final PhysicalPlanExecutor executor = new PhysicalPlanExecutor();

  @Test
  void shouldExecutePhysicalOperator() {
    InMemoryOperator root =
        new InMemoryOperator(List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob")));

    List<RelationalRow> result = executor.execute(root);

    assertEquals(List.of(RelationalRow.of(1, "Alice"), RelationalRow.of(2, "Bob")), result);
    assertTrue(root.closed);
  }

  @Test
  void shouldReturnEmptyResultForEmptyInput() {
    InMemoryOperator root = new InMemoryOperator(List.of());

    assertTrue(executor.execute(root).isEmpty());
    assertTrue(root.closed);
  }

  @Test
  void shouldReturnImmutableResults() {
    InMemoryOperator root = new InMemoryOperator(List.of(RelationalRow.of(1, "Alice")));

    List<RelationalRow> result = executor.execute(root);

    assertThrows(UnsupportedOperationException.class, () -> result.add(RelationalRow.of(2, "Bob")));
  }

  @Test
  void shouldExecuteComposedPhysicalOperatorTree() {
    PhysicalOperator plan =
        new LimitOperator(
            new ProjectOperator(
                new FilterOperator(
                    new InMemoryOperator(
                        List.of(
                            RelationalRow.of(1, "Alice", true),
                            RelationalRow.of(2, "Bob", false),
                            RelationalRow.of(3, "Charlie", true))),
                    row -> (Boolean) row.getValue(2)),
                1,
                0),
            1);

    assertEquals(List.of(RelationalRow.of("Alice", 1)), executor.execute(plan));
  }

  @Test
  void shouldCloseRootOperatorWhenExecutionFails() {
    FailingOperator root = new FailingOperator();

    assertThrows(IllegalStateException.class, () -> executor.execute(root));

    assertTrue(root.closed);
  }

  @Test
  void shouldRejectNullRootOperator() {
    assertThrows(NullPointerException.class, () -> executor.execute(null));
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
      if (opened || closed) {
        throw new IllegalStateException("Operator cannot be opened");
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

  private static final class FailingOperator implements PhysicalOperator {

    private boolean closed;

    @Override
    public RowIterator open() {
      return new RowIterator() {
        @Override
        public boolean hasNext() {
          throw new IllegalStateException("Simulated execution failure");
        }

        @Override
        public RelationalRow next() {
          throw new NoSuchElementException();
        }

        @Override
        public void close() {
          // No resources to release in this test operator.
        }
      };
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
