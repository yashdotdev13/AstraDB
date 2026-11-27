package io.astradb.queryexecutor.storage;

import io.astradb.sql.model.RelationalRow;
import io.astradb.sql.model.SqlTable;
import io.astradb.transaction.Transaction;
import java.util.LinkedHashMap;
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

    validateRow(table, row);

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
    return List.copyOf(scanEntries(transaction, table).values());
  }

  /**
   * Scans table rows while preserving their internal storage keys.
   *
   * @param transaction active transaction
   * @param table table metadata
   * @return map of internal row keys to rows
   */
  public Map<String, RelationalRow> scanEntries(
      Transaction<String, RelationalRow> transaction, SqlTable table) {

    Objects.requireNonNull(transaction, "Transaction must not be null");
    Objects.requireNonNull(table, "Table must not be null");

    String prefix = tablePrefix(table) + "ROW:";
    Map<String, RelationalRow> entries = transaction.scan();
    Map<String, RelationalRow> rows = new LinkedHashMap<>();

    entries.forEach(
        (key, row) -> {
          if (key.startsWith(prefix)) {
            rows.put(key, row);
          }
        });

    return Map.copyOf(rows);
  }

  /**
   * Replaces an existing row without generating a new storage key.
   *
   * @param transaction active transaction
   * @param table table metadata
   * @param key existing internal row key
   * @param row replacement row
   */
  public void update(
      Transaction<String, RelationalRow> transaction,
      SqlTable table,
      String key,
      RelationalRow row) {

    Objects.requireNonNull(transaction, "Transaction must not be null");
    Objects.requireNonNull(table, "Table must not be null");
    Objects.requireNonNull(key, "Row key must not be null");
    Objects.requireNonNull(row, "Row must not be null");

    if (!key.startsWith(tablePrefix(table) + "ROW:")) {
      throw new IllegalArgumentException("Row key does not belong to table: " + table.name());
    }

    validateRow(table, row);
    transaction.put(key, row);
  }

  private void validateRow(SqlTable table, RelationalRow row) {
    if (row.size() != table.columns().size()) {
      throw new IllegalArgumentException("Row value count must match the number of table columns");
    }
  }

  private String tablePrefix(SqlTable table) {
    return KEY_PREFIX + table.name().toUpperCase() + ":";
  }
}
