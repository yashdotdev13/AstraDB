package io.astradb.queryexecutor.storage;

import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Provides relational table access backed by an MVCC transaction.
 *
 * <p>Rows are stored using table-qualified internal keys. Scans use the transaction's snapshot and
 * therefore respect MVCC visibility and transaction-local mutations.
 */
public class TransactionBackedRelationalTableAccess {

  private static final String KEY_PREFIX = "ASTRA_TABLE:";

  /**
   * Inserts a row into the specified table within the transaction.
   *
   * @param transaction active transaction
   * @param table table metadata
   * @param row row to insert
   * @return generated internal row key
   */
  public String insert(
      Transaction<String, RelationalRow> transaction, SqlTable table, RelationalRow row) {

    Objects.requireNonNull(transaction, "Transaction must not be null");
    Objects.requireNonNull(table, "Table must not be null");
    Objects.requireNonNull(row, "Row must not be null");

    if (row.size() != table.columns().size()) {
      throw new IllegalArgumentException("Row value count must match the number of table columns");
    }

    String key = tablePrefix(table) + "ROW:" + UUID.randomUUID();
    transaction.put(key, row);

    return key;
  }

  /**
   * Scans all rows visible to the transaction for the specified table.
   *
   * @param transaction active transaction
   * @param table table metadata
   * @return snapshot-visible rows
   */
  public List<RelationalRow> scan(Transaction<String, RelationalRow> transaction, SqlTable table) {

    Objects.requireNonNull(transaction, "Transaction must not be null");
    Objects.requireNonNull(table, "Table must not be null");

    String prefix = tablePrefix(table);
    Map<String, RelationalRow> entries = transaction.scan();

    List<RelationalRow> rows = new ArrayList<>();

    entries.forEach(
        (key, row) -> {
          if (key.startsWith(prefix + "ROW:")) {
            rows.add(row);
          }
        });

    return List.copyOf(rows);
  }

  private String tablePrefix(SqlTable table) {
    return KEY_PREFIX + table.name().toUpperCase() + ":";
  }
}
