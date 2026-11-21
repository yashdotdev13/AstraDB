package io.astradb.sql.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.common.exception.ErrorCode;
import io.astradb.sql.exception.SqlValidationException;
import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AstraSqlValidatorTest {

  private AstraSqlValidator validator;
  private SqlSchema schema;

  @BeforeEach
  void setUp() {
    validator = new AstraSqlValidator();

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
  void shouldValidateSelectStatement() {
    SqlNode result = validator.validate("SELECT id, name FROM users", schema);

    assertNotNull(result);
  }

  @Test
  void shouldValidateInsertStatement() {
    SqlNode result =
        validator.validate(
            "INSERT INTO users (id, name, email) VALUES (1, 'Yash', 'yash@example.com')", schema);

    assertNotNull(result);
  }

  @Test
  void shouldValidateUpdateStatement() {
    SqlNode result = validator.validate("UPDATE users SET name = 'Yash' WHERE id = 1", schema);

    assertNotNull(result);
  }

  @Test
  void shouldValidateDeleteStatement() {
    SqlNode result = validator.validate("DELETE FROM users WHERE id = 1", schema);

    assertNotNull(result);
  }

  @Test
  void shouldValidateCaseInsensitiveTableAndColumnNames() {
    SqlNode result = validator.validate("SELECT ID, NAME FROM USERS", schema);

    assertNotNull(result);
  }

  @Test
  void shouldRejectUnknownTable() {
    SqlValidationException exception =
        assertThrows(
            SqlValidationException.class,
            () -> validator.validate("SELECT id FROM orders", schema));

    assertEquals(ErrorCode.SQL_VALIDATION_ERROR, exception.getErrorCode());
  }

  @Test
  void shouldRejectUnknownColumn() {
    SqlValidationException exception =
        assertThrows(
            SqlValidationException.class,
            () -> validator.validate("SELECT age FROM users", schema));

    assertEquals(ErrorCode.SQL_VALIDATION_ERROR, exception.getErrorCode());
  }

  @Test
  void shouldRejectInvalidColumnReference() {
    assertThrows(
        SqlValidationException.class,
        () -> validator.validate("SELECT users.age FROM users", schema));
  }

  @Test
  void shouldRejectUnknownInsertColumn() {
    assertThrows(
        SqlValidationException.class,
        () -> validator.validate("INSERT INTO users (id, age) VALUES (1, 25)", schema));
  }

  @Test
  void shouldRejectUnknownUpdateColumn() {
    assertThrows(
        SqlValidationException.class,
        () -> validator.validate("UPDATE users SET age = 25 WHERE id = 1", schema));
  }

  @Test
  void shouldRejectUnknownDeleteConditionColumn() {
    assertThrows(
        SqlValidationException.class,
        () -> validator.validate("DELETE FROM users WHERE age = 25", schema));
  }

  @Test
  void shouldRejectNullSchema() {
    assertThrows(
        NullPointerException.class, () -> validator.validate("SELECT id FROM users", null));
  }
}
