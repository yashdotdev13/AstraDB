package io.astradb.queryexecutor.operator.aggregate;

import java.util.Objects;

public record AggregateCall(AggregateFunction function, int inputColumnIndex) {

  public AggregateCall {
    Objects.requireNonNull(function, "Aggregate function must not be null");

    if (inputColumnIndex < -1) {
      throw new IllegalArgumentException("Input column index must be -1 or non-negative");
    }

    if (inputColumnIndex == -1 && function != AggregateFunction.COUNT) {
      throw new IllegalArgumentException("Only COUNT(*) can use an input column index of -1");
    }
  }

  public static AggregateCall countStar() {
    return new AggregateCall(AggregateFunction.COUNT, -1);
  }

  public static AggregateCall count(int columnIndex) {
    return new AggregateCall(AggregateFunction.COUNT, columnIndex);
  }

  public static AggregateCall sum(int columnIndex) {
    return new AggregateCall(AggregateFunction.SUM, columnIndex);
  }

  public static AggregateCall avg(int columnIndex) {
    return new AggregateCall(AggregateFunction.AVG, columnIndex);
  }

  public static AggregateCall min(int columnIndex) {
    return new AggregateCall(AggregateFunction.MIN, columnIndex);
  }

  public static AggregateCall max(int columnIndex) {
    return new AggregateCall(AggregateFunction.MAX, columnIndex);
  }
}
