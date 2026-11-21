package io.astradb.sql.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RelationalRowTest {

  @Test
  void shouldCreateRowWithPositionalValues() {
    RelationalRow row = RelationalRow.of(1, "Yash", 21);

    assertEquals(3, row.size());
    assertEquals(1, row.getValue(0));
    assertEquals("Yash", row.getValue(1));
    assertEquals(21, row.getValue(2));
  }

  @Test
  void shouldSupportNullValues() {
    RelationalRow row = RelationalRow.of(1, "Yash", null);

    assertEquals(3, row.size());
    assertEquals(null, row.getValue(2));
  }

  @Test
  void shouldCreateImmutableRow() {
    List<Object> values = new ArrayList<>(List.of(1, "Yash"));
    RelationalRow row = new RelationalRow(values);

    values.add(21);

    assertEquals(2, row.size());
    assertThrows(UnsupportedOperationException.class, () -> row.values().add(21));
  }

  @Test
  void shouldRejectNullValuesList() {
    assertThrows(NullPointerException.class, () -> new RelationalRow(null));
  }

  @Test
  void shouldRejectNullVarargsArray() {
    assertThrows(NullPointerException.class, () -> RelationalRow.of((Object[]) null));
  }

  @Test
  void shouldExposeImmutableValues() {
    RelationalRow row = RelationalRow.of("A", "B");

    assertTrue(row.values().contains("A"));
    assertEquals(List.of("A", "B"), row.values());
  }
}
