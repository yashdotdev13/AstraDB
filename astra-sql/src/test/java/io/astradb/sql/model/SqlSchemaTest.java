package io.astradb.sql.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqlSchemaTest {

  private SqlSchema schema;

  @BeforeEach
  void setUp() {
    SqlTable users =
        new SqlTable(
            "users",
            List.of(
                new SqlColumn("id", "BIGINT", false),
                new SqlColumn("name", "VARCHAR", true),
                new SqlColumn("email", "VARCHAR", true)));

    schema = new SqlSchema(List.of(users));
  }

  @Test
  void shouldFindTableByName() {
    assertTrue(schema.findTable("users").isPresent());
  }

  @Test
  void shouldFindTableCaseInsensitively() {
    assertTrue(schema.findTable("USERS").isPresent());
  }

  @Test
  void shouldReturnEmptyForUnknownTable() {
    assertTrue(schema.findTable("orders").isEmpty());
  }

  @Test
  void shouldFindColumnByTableAndColumnName() {
    assertEquals("BIGINT", schema.findColumn("users", "id").orElseThrow().type());
  }

  @Test
  void shouldFindColumnCaseInsensitively() {
    assertTrue(schema.findColumn("USERS", "NAME").isPresent());
  }

  @Test
  void shouldReturnEmptyForUnknownColumn() {
    assertTrue(schema.findColumn("users", "age").isEmpty());
  }

  @Test
  void shouldReturnEmptyForUnknownTableColumnLookup() {
    assertTrue(schema.findColumn("orders", "id").isEmpty());
  }

  @Test
  void shouldRejectDuplicateTableNames() {
    SqlTable duplicate = new SqlTable("USERS", List.of(new SqlColumn("id", "BIGINT", false)));

    assertThrows(
        IllegalArgumentException.class,
        () -> new SqlSchema(List.of(schema.findTable("users").orElseThrow(), duplicate)));
  }

  @Test
  void shouldExposeImmutableTableCollection() {
    assertThrows(UnsupportedOperationException.class, () -> schema.getTables().clear());
  }

  @Test
  void shouldRejectNullTableName() {
    assertThrows(NullPointerException.class, () -> schema.findTable(null));
  }

  @Test
  void shouldRejectNullColumnName() {
    assertThrows(NullPointerException.class, () -> schema.findColumn("users", null));
  }
}
