package io.astradb.queryexecutor.operator;

/**
 * Represents an executable physical relational operator.
 *
 * <p>Physical operators produce relational rows through a {@link RowIterator}.
 */
public interface PhysicalOperator extends AutoCloseable {

  /**
   * Opens the operator and creates an iterator over its output rows.
   *
   * @return iterator over produced relational rows
   */
  RowIterator open();

  /** Releases resources owned by this operator. */
  @Override
  void close();
}
