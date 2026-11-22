package io.astradb.queryexecutor.operator;

import io.astradb.queryexecutor.storage.TransactionBackedRelationalTableAccess;
import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Physical operator that scans all visible rows from a relational table.
 *
 * <p>Rows are read through transaction-aware table access, ensuring that snapshot visibility and
 * pending transaction mutations are respected.
 */
public final class TableScanOperator implements PhysicalOperator {

  private final TransactionBackedRelationalTableAccess tableAccess;
  private final Transaction<String, RelationalRow> transaction;
  private final SqlTable table;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  public TableScanOperator(
      TransactionBackedRelationalTableAccess tableAccess,
      Transaction<String, RelationalRow> transaction,
      SqlTable table) {

    this.tableAccess = Objects.requireNonNull(tableAccess, "Table access must not be null");
    this.transaction = Objects.requireNonNull(transaction, "Transaction must not be null");
    this.table = Objects.requireNonNull(table, "Table must not be null");
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Table scan operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Table scan operator has already been opened");
    }

    List<RelationalRow> rows = tableAccess.scan(transaction, table);

    activeIterator = new ListRowIterator(rows);
    opened = true;

    return activeIterator;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    if (activeIterator != null) {
      activeIterator.close();
    }
  }

  private static final class ListRowIterator implements RowIterator {

    private final java.util.Iterator<RelationalRow> delegate;
    private boolean closed;

    private ListRowIterator(List<RelationalRow> rows) {
      this.delegate = rows.iterator();
    }

    @Override
    public boolean hasNext() {
      return !closed && delegate.hasNext();
    }

    @Override
    public RelationalRow next() {
      if (closed || !delegate.hasNext()) {
        throw new NoSuchElementException("No more rows available");
      }

      return delegate.next();
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
