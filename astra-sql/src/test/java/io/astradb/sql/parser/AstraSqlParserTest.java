package io.astradb.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.common.exception.ErrorCode;
import io.astradb.sql.exception.SqlParseException;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AstraSqlParserTest {

  private AstraSqlParser parser;

  @BeforeEach
  void setUp() {
    parser = new AstraSqlParser();
  }

  @Test
  void shouldParseSelectStatement() {
    SqlNode result = parser.parse("SELECT id, name FROM users");

    assertNotNull(result);
    assertEquals("SELECT", result.getKind().name());
  }

  @Test
  void shouldParseInsertStatement() {
    SqlNode result = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Yash')");

    assertNotNull(result);
    assertEquals("INSERT", result.getKind().name());
  }

  @Test
  void shouldRejectInvalidSqlSyntax() {
    SqlParseException exception =
        assertThrows(SqlParseException.class, () -> parser.parse("SELECT FROM WHERE"));

    assertEquals(ErrorCode.SQL_PARSE_ERROR, exception.getErrorCode());
    assertNotNull(exception.getCause());
  }

  @Test
  void shouldRejectBlankSql() {
    assertThrows(IllegalArgumentException.class, () -> parser.parse("   "));
  }

  @Test
  void shouldRejectNullSql() {
    assertThrows(NullPointerException.class, () -> parser.parse(null));
  }
}
