from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VLM_TYPE,
)


class SqlValueCheckAgainstDefineVLMDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    is a row in the new dataset. The Define XML VLM metadata corresponding to each row's value is attached.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_value_check_define_vlm"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        define_vlms = self.get_define_vlms()
        define_vlm_by_var = {v.get("define_variable_name", "").upper(): v for v in define_vlms}

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        schema.add_column(SqlColumnSchema.generated("define_variable_name", "Char"))
        for col, type in DEFINE_VLM_TYPE.items():
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
                d_var = define_vlm_by_var.get(col_upper, {})

                row_parts = [f"'{col_upper}'"]
                for key in DEFINE_VLM_TYPE.keys():
                    val = str(d_var.get(key, "")).replace("'", "''").strip()
                    row_parts.append("NULL" if val.lower() in ["", "none", "null"] else f"'{val}'")

                val_rows.append(f"({', '.join(row_parts)})")

            values_sql = ",\n".join(val_rows)

            target_columns = ["row_number", "variable_name", "variable_value", "define_variable_name"] + list(
                DEFINE_VLM_TYPE.keys()
            )
            columns_clause = ", ".join([schema.get_column_hash(col) for col in target_columns])
            select_cols = ", ".join([f"CAST(m.{col} AS TEXT)" for col in DEFINE_VLM_TYPE.keys()])

            insert_query = f"""
                WITH vlm_meta AS (
                    SELECT * FROM (VALUES
                        {values_sql}
                    ) AS m(var_name, {", ".join(DEFINE_VLM_TYPE.keys())})
                )
                INSERT INTO {schema.hash} ({columns_clause})
                SELECT
                    CAST(ROW_NUMBER() OVER () AS TEXT) as row_number,
                    j.key as variable_name,
                    j.value as variable_value,
                    m.var_name as define_variable_name,
                    {select_cols}
                FROM {source_table_hash} t,
                LATERAL jsonb_each_text({json_build_str}) AS j(key, value)
                LEFT JOIN vlm_meta m ON j.key = m.var_name;
            """

            self.data_service.pgi.execute_sql(insert_query)

        return table_name
