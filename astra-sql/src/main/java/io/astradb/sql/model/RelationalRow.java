package io.astradb.sql.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an immutable relational row containing positional column values.
 *
 * <p>The position of each value corresponds to the position of its column in the associated {@link
 * SqlTable}. Null values are supported for nullable SQL columns.
 */
public record RelationalRow(List<Object> values) {

  public RelationalRow {
    Objects.requireNonNull(values, "Row values must not be null");
    values = Collections.unmodifiableList(new ArrayList<>(values));
  }

  /**
   * Creates a relational row from the supplied values.
   *
   * @param values positional column values
   * @return a new relational row
   */
  public static RelationalRow of(Object... values) {
    Objects.requireNonNull(values, "Row values must not be null");
    return new RelationalRow(Arrays.asList(values));
  }

  /**
   * Returns the value at the specified column position.
   *
   * @param index zero-based column position
   * @return the column value, which may be null
   */
  public Object getValue(int index) {
    return values.get(index);
  }

  /**
   * Returns the number of values in this row.
   *
   * @return number of column values
   */
  public int size() {
    return values.size();
  }
}
