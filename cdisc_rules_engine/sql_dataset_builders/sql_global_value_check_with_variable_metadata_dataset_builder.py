from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder


class SqlGlobalValueCheckwithVariableMetadataDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Global Value Check with Variable Metadata rules.
    Converts all datasets from wide to long format (unpivots) and attaches variable metadata.
    """

    def build(self) -> str:
        table_name = "global_value_check"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        all_ds_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_length", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value_length", "Num"))

        self.data_service.pgi.create_table(schema)

        for ds_metadata in all_ds_metadata:
            ds_table_hash = self.data_service.pgi.schema.get_table_hash(ds_metadata.name)
            if not ds_table_hash:
                continue

            ds_schema = self.data_service.pgi.schema.get_table(ds_metadata.name)

            var_metadata = {
                var.name.upper(): {
                    "label": var.label or "",
                    "data_type": var.type or "",
                    "length": var.length or 0,
                    "order": var.order or 0,
                    "format": var.format or "",
                }
                for var in ds_metadata.variables
            }

            columns_list = ds_schema.get_columns()
            column_names = [
                name
                for name, schema in columns_list
                if name.lower() not in ["id", "source_ds", "source_row_number"] and schema.origin == "data"
            ]

            if column_names:
                json_build_str = self.generate_unpivot_jsonb_string(ds_schema, column_names)

                var_values = []
                for col_name in column_names:
                    col_upper = col_name.upper()
                    meta = var_metadata.get(col_upper, {})
                    var_label = meta.get("label", "").replace("'", "''")
                    var_data_type = meta.get("data_type", "").replace("'", "''")
                    var_length = meta.get("length", 0)
                    var_order = meta.get("order", 0)
                    var_format = meta.get("format", "").replace("'", "''")

                    var_values.append(
                        f"('{col_upper}', '{var_label}', '{var_data_type}', {var_length}, {var_order}, '{var_format}')"
                    )

                values_sql = ",\n".join(var_values)

                insert_query = f"""
                    WITH var_meta AS (
                        SELECT * FROM (VALUES
                            {values_sql}
                        ) AS m(var_name, var_label, var_data_type, var_length, var_order, var_format)
                    )
                    INSERT INTO {schema.hash}
                    ({schema.get_column_hash("row_number")},
                     {schema.get_column_hash("dataset_name")},
                     {schema.get_column_hash("variable_name")},
                     {schema.get_column_hash("variable_value")},
                     {schema.get_column_hash("variable_label")},
                     {schema.get_column_hash("variable_data_type")},
                     {schema.get_column_hash("variable_length")},
                     {schema.get_column_hash("variable_order_number")},
                     {schema.get_column_hash("variable_format")},
                     {schema.get_column_hash("variable_value_length")})
                    SELECT
                        ROW_NUMBER() OVER () as row_number,
                        '{ds_metadata.name}' as dataset_name,
                        j.key as variable_name,
                        j.value as variable_value,
                        m.var_label as variable_label,
                        m.var_data_type as variable_data_type,
                        m.var_length as variable_length,
                        m.var_order as variable_order_number,
                        m.var_format as variable_format,
                        CASE
                            WHEN m.var_data_type = 'integer' THEN LENGTH(LTRIM(j.value, '0'))
                            WHEN m.var_data_type = 'float' THEN LENGTH(REPLACE(LTRIM(j.value, '0'), '.', ''))
                            ELSE LENGTH(j.value)
                        END as variable_value_length
                    FROM {ds_table_hash} t,
                    LATERAL jsonb_each_text({json_build_str}) AS j(key, value)
                    LEFT JOIN var_meta m ON j.key = m.var_name;
                """

                self.data_service.pgi.execute_sql(insert_query)

        return table_name
