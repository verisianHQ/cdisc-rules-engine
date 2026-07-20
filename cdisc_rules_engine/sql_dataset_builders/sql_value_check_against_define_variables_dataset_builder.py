from typing import Tuple, Optional

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
)


class SqlValueCheckAgainstDefineVariablesDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    is a row in the new dataset. The Define XML variable metadata corresponding to each row's
    variable is attached.
    """

    def build(self) -> Tuple[str, Optional[str]]:
        table_name = f"{self.dataset_metadata.name}_value_check_define_variables"
        existing_schema = self.data_service.pgi.schema.get_table(table_name)
        if existing_schema is not None:
            return table_name, f"SELECT * FROM {existing_schema.hash}"

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        define_vars = self.get_define_vars()
        define_vars_by_name = {v.get("define_variable_name", "").upper(): v for v in define_vars}

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        for col, type in DEFINE_VARIABLES_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)

        columns_list = source_schema.get_columns()
        column_names = [
            name
            for name, sc in columns_list
            if name.lower() not in ["id", "source_ds", "source_row_number"] and sc.origin == "data"
        ]

        if column_names:
            json_build_str = self.generate_unpivot_jsonb_string(source_schema, column_names)

            val_rows = []
            for col_name in column_names:
                col_upper = col_name.upper()
                d_var = define_vars_by_name.get(col_upper, {})

                row_parts = [f"'{col_upper}'"]
                for key in DEFINE_VARIABLES_TYPE.keys():
                    val = str(d_var.get(key, "")).replace("'", "''").strip()
                    column_type = DEFINE_VARIABLES_TYPE[key]

                    if column_type == "Bool":
                        sql_val = "NULL" if val.lower() in ["", "none", "null"] else f"CAST({val} AS BOOLEAN)"
                    elif column_type == "Num":
                        sql_val = "NULL" if val.lower() in ["", "none", "null"] else f"CAST({val} AS NUMERIC)"
                    else:
                        sql_val = "NULL" if val.lower() in ["", "none", "null"] else f"'{val}'"
                    row_parts.append(sql_val)

                val_rows.append(f"({', '.join(row_parts)})")

            values_sql = ",\n".join(val_rows)
            target_columns = ["row_number", "variable_name", "variable_value"] + list(DEFINE_VARIABLES_TYPE.keys())
            columns_clause = ", ".join([schema.get_column_hash(col) for col in target_columns])
            select_cols = ", ".join([f"m.{col}" for col in DEFINE_VARIABLES_TYPE.keys()])

            insert_query = f"""
                WITH def_meta AS (
                    SELECT * FROM (VALUES
                        {values_sql}
                    ) AS m(var_name, {", ".join(DEFINE_VARIABLES_TYPE.keys())})
                )
                INSERT INTO {schema.hash} ({columns_clause})
                SELECT
                    CAST(ROW_NUMBER() OVER () AS TEXT) as row_number,
                    j.key as variable_name,
                    j.value as variable_value,
                    {select_cols}
                FROM {source_table_hash} t,
                LATERAL jsonb_each_text({json_build_str}) AS j(key, value)
                LEFT JOIN def_meta m ON j.key = m.var_name;
            """

            self.data_service.pgi.execute_sql(insert_query)

        return table_name, f"SELECT * FROM {table_name}"
