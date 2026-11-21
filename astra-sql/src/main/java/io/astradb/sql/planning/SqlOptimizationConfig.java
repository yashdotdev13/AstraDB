package io.astradb.sql.planning;

/**
 * Configuration for SQL logical plan optimization.
 *
 * @param enableFilterProjectTranspose enables filter pushdown through projections
 * @param enableProjectMerge enables merging consecutive projection operators
 * @param enableFilterMerge enables merging consecutive filter operators
 * @param enableConstantReduction enables constant expression reduction
 */
public record SqlOptimizationConfig(
    boolean enableFilterProjectTranspose,
    boolean enableProjectMerge,
    boolean enableFilterMerge,
    boolean enableConstantReduction) {

  /**
   * Creates the default SQL optimization configuration.
   *
   * @return default optimization configuration
   */
  public static SqlOptimizationConfig defaults() {
    return new SqlOptimizationConfig(true, true, true, true);
  }
}
