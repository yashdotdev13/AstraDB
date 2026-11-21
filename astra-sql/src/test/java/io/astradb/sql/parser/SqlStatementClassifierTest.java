package io.astradb.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.sql.model.SqlStatementType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqlStatementClassifierTest {

  private AstraSqlParser parser;
  private SqlStatementClassifier classifier;

  @BeforeEach
  void setUp() {
    parser = new AstraSqlParser();
    classifier = new SqlStatementClassifier();
  }

  @Test
  void shouldClassifySelectStatement() {
    assertEquals(SqlStatementType.SELECT, classifier.classify(parser.parse("SELECT * FROM users")));
  }

  @Test
  void shouldClassifyInsertStatement() {
    assertEquals(
        SqlStatementType.INSERT,
        classifier.classify(parser.parse("INSERT INTO users (id) VALUES (1)")));
  }

  @Test
  void shouldClassifyUpdateStatement() {
    assertEquals(
        SqlStatementType.UPDATE,
        classifier.classify(parser.parse("UPDATE users SET name = 'Yash' WHERE id = 1")));
  }

  @Test
  void shouldClassifyDeleteStatement() {
    assertEquals(
        SqlStatementType.DELETE,
        classifier.classify(parser.parse("DELETE FROM users WHERE id = 1")));
  }

  @Test
  void shouldClassifyMergeStatement() {
    assertEquals(
        SqlStatementType.MERGE,
        classifier.classify(
            parser.parse(
                "MERGE INTO target USING source ON target.id = source.id "
                    + "WHEN MATCHED THEN UPDATE SET target.name = source.name")));
  }

  @Test
  void shouldRejectNullSqlNode() {
    assertThrows(NullPointerException.class, () -> classifier.classify(null));
  }
}
