package io.astradb.queryexecutor.operator;

import io.astradb.sql.model.RelationalRow;
import java.util.Iterator;

/**
 * Iterates over relational rows produced by a physical execution operator.
 *
 * <p>Implementations may own resources that must be released after iteration.
 */
public interface RowIterator extends Iterator<RelationalRow>, AutoCloseable {

  /**
   * Releases resources associated with this iterator.
   *
   * <p>Closing an iterator multiple times should be safe.
   */
  @Override
  void close();
}
