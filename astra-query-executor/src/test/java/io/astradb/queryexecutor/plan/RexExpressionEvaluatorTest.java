package io.astradb.queryexecutor.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.astradb.sql.model.RelationalRow;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RexExpressionEvaluatorTest {

  private RexExpressionEvaluator evaluator;
  private RexBuilder rexBuilder;

  @BeforeEach
  void setUp() {
    evaluator = new RexExpressionEvaluator();
    rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
  }

  @Test
  void shouldEvaluateGreaterThanExpression() {
    RexNode expression = comparison(SqlStdOperatorTable.GREATER_THAN, 0, 10);

    assertEquals(true, evaluator.evaluate(expression, RelationalRow.of(20)));
  }

  @Test
  void shouldEvaluateLessThanExpression() {
    RexNode expression = comparison(SqlStdOperatorTable.LESS_THAN, 0, 10);

    assertEquals(true, evaluator.evaluate(expression, RelationalRow.of(5)));
  }

  @Test
  void shouldEvaluateEqualityExpression() {
    RexNode expression = comparison(SqlStdOperatorTable.EQUALS, 0, 10);

    assertEquals(true, evaluator.evaluate(expression, RelationalRow.of(10)));
  }

  @Test
  void shouldReturnFalseWhenGreaterThanConditionIsNotSatisfied() {
    RexNode expression = comparison(SqlStdOperatorTable.GREATER_THAN, 0, 10);

    assertEquals(false, evaluator.evaluate(expression, RelationalRow.of(5)));
  }

  @Test
  void shouldReturnUnknownWhenComparingNull() {
    RexNode expression = comparison(SqlStdOperatorTable.GREATER_THAN, 0, 10);

    assertEquals(null, evaluator.evaluate(expression, RelationalRow.of((Object) null)));
  }

  @Test
  void shouldRejectUnsupportedExpression() {
    RexNode expression =
        rexBuilder.makeCall(
            SqlStdOperatorTable.ABS, rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE));

    assertThrows(
        UnsupportedOperationException.class,
        () -> evaluator.evaluate(expression, RelationalRow.of()));
  }

  @Test
  void shouldEvaluateAddition() {
    RexNode expression = arithmetic(SqlStdOperatorTable.PLUS, 0, 10);

    assertEquals(
        new java.math.BigDecimal("20"), evaluator.evaluate(expression, RelationalRow.of(10)));
  }

  @Test
  void shouldEvaluateSubtraction() {
    RexNode expression = arithmetic(SqlStdOperatorTable.MINUS, 0, 5);

    assertEquals(
        new java.math.BigDecimal("15"), evaluator.evaluate(expression, RelationalRow.of(20)));
  }

  @Test
  void shouldEvaluateMultiplication() {
    RexNode expression = arithmetic(SqlStdOperatorTable.MULTIPLY, 0, 3);

    assertEquals(
        new java.math.BigDecimal("30"), evaluator.evaluate(expression, RelationalRow.of(10)));
  }

  @Test
  void shouldEvaluateDivision() {
    RexNode expression = arithmetic(SqlStdOperatorTable.DIVIDE, 0, 2);

    assertEquals(
        new java.math.BigDecimal("5"), evaluator.evaluate(expression, RelationalRow.of(10)));
  }

  @Test
  void shouldReturnNullWhenArithmeticOperandIsNull() {
    RexNode expression = arithmetic(SqlStdOperatorTable.PLUS, 0, 10);

    assertEquals(null, evaluator.evaluate(expression, RelationalRow.of((Object) null)));
  }

  @Test
  void shouldRejectDivisionByZero() {
    RexNode expression = arithmetic(SqlStdOperatorTable.DIVIDE, 0, 0);

    assertThrows(
        ArithmeticException.class, () -> evaluator.evaluate(expression, RelationalRow.of(10)));
  }

  @Test
  void shouldRejectNonNumericArithmeticOperands() {
    RexNode expression = arithmetic(SqlStdOperatorTable.PLUS, 0, 10);

    assertThrows(
        IllegalArgumentException.class,
        () -> evaluator.evaluate(expression, RelationalRow.of("Alice")));
  }

  private RexNode arithmetic(
      org.apache.calcite.sql.SqlOperator operator, int columnIndex, int literalValue) {

    RexNode input =
        rexBuilder.makeInputRef(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), columnIndex);

    RexNode literal = rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(literalValue));

    return rexBuilder.makeCall(operator, input, literal);
  }

  private RexNode comparison(
      org.apache.calcite.sql.SqlOperator operator, int columnIndex, int literalValue) {

    RexNode input =
        rexBuilder.makeInputRef(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), columnIndex);

    RexNode literal = rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(literalValue));

    return rexBuilder.makeCall(operator, input, literal);
  }
}
