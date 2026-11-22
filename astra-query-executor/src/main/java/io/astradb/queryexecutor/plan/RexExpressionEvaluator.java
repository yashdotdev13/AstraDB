package io.astradb.queryexecutor.plan;

import io.astradb.sql.model.RelationalRow;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;

/** Evaluates a supported subset of Calcite expressions against relational rows. */
public final class RexExpressionEvaluator {

  private static final MathContext DIVISION_CONTEXT = MathContext.DECIMAL128;

  public Object evaluate(RexNode expression, RelationalRow row) {
    Objects.requireNonNull(expression, "Expression must not be null");
    Objects.requireNonNull(row, "Row must not be null");

    if (expression instanceof RexInputRef inputRef) {
      return row.getValue(inputRef.getIndex());
    }

    if (expression instanceof RexLiteral literal) {
      return evaluateLiteral(literal);
    }

    if (expression instanceof RexCall call) {
      return evaluateCall(call, row);
    }

    throw new UnsupportedOperationException(
        "Unsupported Calcite expression: " + expression.getClass().getName());
  }

  private Object evaluateLiteral(RexLiteral literal) {
    if (literal.isNull()) {
      return null;
    }

    Object value = literal.getValue3();

    if (value instanceof NlsString nlsString) {
      return nlsString.getValue();
    }

    if (value instanceof ByteString byteString) {
      return byteString.getBytes();
    }

    return value;
  }

  private Object evaluateCall(RexCall call, RelationalRow row) {
    List<RexNode> operands = call.getOperands();

    return switch (call.getKind()) {
      case AND -> evaluateAnd(operands, row);
      case OR -> evaluateOr(operands, row);
      case NOT -> negate(asBoolean(evaluate(operands.get(0), row)));
      case EQUALS -> compare(operands, row, Comparison.EQUALS);
      case NOT_EQUALS -> compare(operands, row, Comparison.NOT_EQUALS);
      case GREATER_THAN -> compare(operands, row, Comparison.GREATER_THAN);
      case GREATER_THAN_OR_EQUAL -> compare(operands, row, Comparison.GREATER_THAN_OR_EQUAL);
      case LESS_THAN -> compare(operands, row, Comparison.LESS_THAN);
      case LESS_THAN_OR_EQUAL -> compare(operands, row, Comparison.LESS_THAN_OR_EQUAL);
      case IS_NULL -> evaluate(operands.get(0), row) == null;
      case IS_NOT_NULL -> evaluate(operands.get(0), row) != null;
      case PLUS -> evaluateArithmetic(operands, row, Arithmetic.ADD);
      case MINUS -> evaluateArithmetic(operands, row, Arithmetic.SUBTRACT);
      case TIMES -> evaluateArithmetic(operands, row, Arithmetic.MULTIPLY);
      case DIVIDE -> evaluateArithmetic(operands, row, Arithmetic.DIVIDE);
      default ->
          throw new UnsupportedOperationException(
              "Unsupported SQL expression kind: " + call.getKind());
    };
  }

  private Object evaluateArithmetic(
      List<RexNode> operands, RelationalRow row, Arithmetic arithmetic) {

    if (operands.size() != 2) {
      throw new IllegalArgumentException(
          "Arithmetic operator requires exactly two operands: " + arithmetic);
    }

    Object left = evaluate(operands.get(0), row);
    Object right = evaluate(operands.get(1), row);

    // SQL arithmetic involving NULL produces NULL.
    if (left == null || right == null) {
      return null;
    }

    if (!(left instanceof Number leftNumber) || !(right instanceof Number rightNumber)) {
      throw new IllegalArgumentException(
          "Arithmetic operations require numeric operands but received "
              + left.getClass().getName()
              + " and "
              + right.getClass().getName());
    }

    BigDecimal leftDecimal = toBigDecimal(leftNumber);
    BigDecimal rightDecimal = toBigDecimal(rightNumber);

    return switch (arithmetic) {
      case ADD -> leftDecimal.add(rightDecimal);
      case SUBTRACT -> leftDecimal.subtract(rightDecimal);
      case MULTIPLY -> leftDecimal.multiply(rightDecimal);
      case DIVIDE -> leftDecimal.divide(rightDecimal, DIVISION_CONTEXT);
    };
  }

  private Boolean evaluateAnd(List<RexNode> operands, RelationalRow row) {
    boolean hasNull = false;

    for (RexNode operand : operands) {
      Boolean value = asBoolean(evaluate(operand, row));

      if (Boolean.FALSE.equals(value)) {
        return false;
      }

      if (value == null) {
        hasNull = true;
      }
    }

    return hasNull ? null : true;
  }

  private Boolean evaluateOr(List<RexNode> operands, RelationalRow row) {
    boolean hasNull = false;

    for (RexNode operand : operands) {
      Boolean value = asBoolean(evaluate(operand, row));

      if (Boolean.TRUE.equals(value)) {
        return true;
      }

      if (value == null) {
        hasNull = true;
      }
    }

    return hasNull ? null : false;
  }

  private Boolean compare(List<RexNode> operands, RelationalRow row, Comparison comparison) {
    Object left = evaluate(operands.get(0), row);
    Object right = evaluate(operands.get(1), row);

    if (left == null || right == null) {
      return null;
    }

    int result = compareValues(left, right);

    return switch (comparison) {
      case EQUALS -> result == 0;
      case NOT_EQUALS -> result != 0;
      case GREATER_THAN -> result > 0;
      case GREATER_THAN_OR_EQUAL -> result >= 0;
      case LESS_THAN -> result < 0;
      case LESS_THAN_OR_EQUAL -> result <= 0;
    };
  }

  private int compareValues(Object left, Object right) {
    if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
      return toBigDecimal(leftNumber).compareTo(toBigDecimal(rightNumber));
    }

    if (left instanceof Comparable<?> && left.getClass().isInstance(right)) {
      @SuppressWarnings("unchecked")
      Comparable<Object> comparable = (Comparable<Object>) left;
      return comparable.compareTo(right);
    }

    throw new IllegalArgumentException(
        "Cannot compare values of types "
            + left.getClass().getName()
            + " and "
            + right.getClass().getName());
  }

  private BigDecimal toBigDecimal(Number number) {
    return number instanceof BigDecimal decimal ? decimal : new BigDecimal(number.toString());
  }

  private Boolean asBoolean(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Boolean booleanValue) {
      return booleanValue;
    }

    throw new IllegalArgumentException(
        "Expected a boolean expression but got: " + value.getClass().getName());
  }

  private Boolean negate(Boolean value) {
    return value == null ? null : !value;
  }

  private enum Arithmetic {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE
  }

  private enum Comparison {
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL
  }
}
