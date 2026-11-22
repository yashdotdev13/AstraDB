package io.astradb.queryexecutor.operator.sort;

import java.util.Objects;

/** Describes one column used for physical row sorting. */
public record SortKey(int columnIndex, Direction direction, NullDirection nullDirection) {

  public SortKey {
    if (columnIndex < 0) {
      throw new IllegalArgumentException("Column index must not be negative");
    }

    Objects.requireNonNull(direction, "Sort direction must not be null");
    Objects.requireNonNull(nullDirection, "Null direction must not be null");
  }

  public enum Direction {
    ASCENDING,
    DESCENDING
  }

  public enum NullDirection {
    FIRST,
    LAST
  }
}
