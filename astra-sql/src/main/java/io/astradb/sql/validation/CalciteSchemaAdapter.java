package io.astradb.sql.validation;

import io.astradb.sql.model.SqlColumn;
import io.astradb.sql.model.SqlSchema;
import io.astradb.sql.model.SqlTable;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Adapts AstraDB schema metadata to Apache Calcite's schema model. */
public class CalciteSchemaAdapter extends AbstractSchema {

  private final SqlSchema sqlSchema;

  public CalciteSchemaAdapter(SqlSchema sqlSchema) {
    this.sqlSchema = Objects.requireNonNull(sqlSchema, "SQL schema must not be null");
  }

  @Override
  protected Map<String, Table> getTableMap() {
    Map<String, Table> tables = new LinkedHashMap<>();

    for (SqlTable table : sqlSchema.getTables()) {
      tables.put(normalize(table.name()), new CalciteTable(table));
    }

    return Map.copyOf(tables);
  }

  private static String normalize(String identifier) {
    return identifier.toUpperCase(Locale.ROOT);
  }

  private static class CalciteTable extends AbstractTable {

    private final SqlTable table;

    private CalciteTable(SqlTable table) {
      this.table = table;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      for (SqlColumn column : table.columns()) {
        SqlTypeName sqlTypeName = SqlTypeName.valueOf(normalize(column.type()));

        RelDataType dataType = typeFactory.createSqlType(sqlTypeName);
        dataType = typeFactory.createTypeWithNullability(dataType, column.nullable());

        builder.add(normalize(column.name()), dataType);
      }

      return builder.build();
    }
  }
}
