package io.astradb.queryexecutor.operator.join;

import io.astradb.queryexecutor.operator.PhysicalOperator;
import io.astradb.queryexecutor.operator.RowIterator;
import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Physical operator that executes an INNER equi-join using a hash-based algorithm.
 *
 * <p>The right input is used to build a hash table. The left input is then used to probe the hash
 * table. Matching rows are emitted with left-side values followed by right-side values.
 *
 * <p>Supports single-column and composite join keys. NULL join keys do not match, following SQL
 * equality semantics.
 */
public final class HashJoinOperator implements PhysicalOperator {

  private final PhysicalOperator leftInput;
  private final PhysicalOperator rightInput;
  private final List<Function<RelationalRow, Object>> leftKeyExtractors;
  private final List<Function<RelationalRow, Object>> rightKeyExtractors;

  private RowIterator activeIterator;
  private boolean opened;
  private boolean closed;

  /**
   * Creates a single-column INNER HASH JOIN operator.
   *
   * @param leftInput left input operator
   * @param rightInput right input operator
   * @param leftKeyExtractor extracts the join key from a left row
   * @param rightKeyExtractor extracts the join key from a right row
   */
  public HashJoinOperator(
      PhysicalOperator leftInput,
      PhysicalOperator rightInput,
      Function<RelationalRow, Object> leftKeyExtractor,
      Function<RelationalRow, Object> rightKeyExtractor) {

    this(
        leftInput,
        rightInput,
        List.of(Objects.requireNonNull(leftKeyExtractor, "Left key extractor must not be null")),
        List.of(Objects.requireNonNull(rightKeyExtractor, "Right key extractor must not be null")));
  }

  /**
   * Creates a single-column or composite-key INNER HASH JOIN operator.
   *
   * @param leftInput left input operator
   * @param rightInput right input operator
   * @param leftKeyExtractors extractors for left-side join key components
   * @param rightKeyExtractors extractors for right-side join key components
   */
  public HashJoinOperator(
      PhysicalOperator leftInput,
      PhysicalOperator rightInput,
      List<Function<RelationalRow, Object>> leftKeyExtractors,
      List<Function<RelationalRow, Object>> rightKeyExtractors) {

    this.leftInput = Objects.requireNonNull(leftInput, "Left input must not be null");
    this.rightInput = Objects.requireNonNull(rightInput, "Right input must not be null");

    this.leftKeyExtractors =
        List.copyOf(
            Objects.requireNonNull(leftKeyExtractors, "Left key extractors must not be null"));

    this.rightKeyExtractors =
        List.copyOf(
            Objects.requireNonNull(rightKeyExtractors, "Right key extractors must not be null"));

    if (this.leftKeyExtractors.isEmpty()
        || this.leftKeyExtractors.size() != this.rightKeyExtractors.size()) {
      throw new IllegalArgumentException("Join key extractors must have matching non-zero sizes");
    }
  }

  @Override
  public RowIterator open() {
    if (closed) {
      throw new IllegalStateException("Hash join operator is already closed");
    }

    if (opened) {
      throw new IllegalStateException("Hash join operator has already been opened");
    }

    opened = true;

    try {
      Map<List<Object>, List<RelationalRow>> hashTable = buildHashTable();
      List<RelationalRow> leftRows = readAll(leftInput);
      List<RelationalRow> result = new ArrayList<>();

      for (RelationalRow leftRow : leftRows) {
        List<Object> key = extractKey(leftRow, leftKeyExtractors);

        // SQL equality does not match rows containing NULL join-key components.
        if (key == null) {
          continue;
        }

        List<RelationalRow> matches = hashTable.get(key);

        if (matches == null) {
          continue;
        }

        for (RelationalRow rightRow : matches) {
          result.add(combine(leftRow, rightRow));
        }
      }

      activeIterator = new ListRowIterator(result);
      return activeIterator;
    } catch (RuntimeException exception) {
      close();
      throw exception;
    }
  }

  private Map<List<Object>, List<RelationalRow>> buildHashTable() {
    Map<List<Object>, List<RelationalRow>> hashTable = new HashMap<>();

    for (RelationalRow rightRow : readAll(rightInput)) {
      List<Object> key = extractKey(rightRow, rightKeyExtractors);

      if (key == null) {
        continue;
      }

      hashTable.computeIfAbsent(key, ignored -> new ArrayList<>()).add(rightRow);
    }

    return hashTable;
  }

  private List<Object> extractKey(
      RelationalRow row, List<Function<RelationalRow, Object>> extractors) {

    List<Object> key = new ArrayList<>(extractors.size());

    for (Function<RelationalRow, Object> extractor : extractors) {
      Object value = normalizeKey(extractor.apply(row));

      if (value == null) {
        return null;
      }

      key.add(value);
    }

    return List.copyOf(key);
  }

  private List<RelationalRow> readAll(PhysicalOperator input) {
    List<RelationalRow> rows = new ArrayList<>();

    try (RowIterator iterator = input.open()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }

    return rows;
  }

  private Object normalizeKey(Object key) {
    if (key instanceof Number number) {
      return new BigDecimal(number.toString()).stripTrailingZeros();
    }

    return key;
  }

  private RelationalRow combine(RelationalRow leftRow, RelationalRow rightRow) {
    List<Object> values = new ArrayList<>(leftRow.size() + rightRow.size());
    values.addAll(leftRow.values());
    values.addAll(rightRow.values());
    return new RelationalRow(values);
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

    leftInput.close();
    rightInput.close();
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
