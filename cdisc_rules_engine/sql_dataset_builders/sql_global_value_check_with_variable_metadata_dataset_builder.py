from typing import Tuple
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder


class SqlGlobalValueCheckwithVariableMetadataDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Global Value Check with Variable Metadata rules.
    Converts all datasets from wide to long format (unpivots) and attaches variable metadata.
    """

    def build(self) -> Tuple[str, str]:
        table_name = "global_value_check"
        existing_schema = self.data_service.pgi.schema.get_table(table_name)
        if existing_schema is not None:
            return table_name, f"SELECT * FROM {existing_schema.hash}"

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

        self.data_service.pgi.schema.add_table(schema)

        all_ds_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]

        dataset_queries = []

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
                for name, col_schema in columns_list
                if name.lower() not in ["id", "source_ds", "source_row_number"] and col_schema.origin == "data"
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

                select_query = f"""
                    SELECT
                        ROW_NUMBER() OVER () as {schema.get_column_hash("row_number")},
                        '{ds_metadata.name}' as {schema.get_column_hash("dataset_name")},
                        j.key as {schema.get_column_hash("variable_name")},
                        j.value as {schema.get_column_hash("variable_value")},
                        m.var_label as {schema.get_column_hash("variable_label")},
                        m.var_data_type as {schema.get_column_hash("variable_data_type")},
                        m.var_length as {schema.get_column_hash("variable_length")},
                        m.var_order as {schema.get_column_hash("variable_order_number")},
                        m.var_format as {schema.get_column_hash("variable_format")},
                        CASE
                            WHEN m.var_data_type = 'integer' THEN LENGTH(LTRIM(j.value, '0'))
                            WHEN m.var_data_type = 'float' THEN LENGTH(REPLACE(LTRIM(j.value, '0'), '.', ''))
                            ELSE LENGTH(j.value)
                        END as {schema.get_column_hash("variable_value_length")}
                    FROM {ds_table_hash} t,
                    LATERAL jsonb_each_text({json_build_str}) AS j(key, value)
                    LEFT JOIN (VALUES
                        {values_sql}
                    ) AS m(var_name, var_label, var_data_type, var_length, var_order, var_format)
                        ON j.key = m.var_name
                """

                dataset_queries.append(select_query)

        if dataset_queries:
            unioned_query = " \nUNION ALL\n ".join(dataset_queries)
            view_select = f"""
                SELECT
                    ROW_NUMBER() OVER () as id,
                    u.*
                FROM (
                    {unioned_query}
                ) u
            """
        else:
            view_select = f"""
                SELECT
                    1::bigint as id,
                    1::numeric as {schema.get_column_hash("row_number")},
                    ''::text as {schema.get_column_hash("dataset_name")},
                    ''::text as {schema.get_column_hash("variable_name")},
                    ''::text as {schema.get_column_hash("variable_value")},
                    ''::text as {schema.get_column_hash("variable_label")},
                    ''::text as {schema.get_column_hash("variable_data_type")},
                    1::numeric as {schema.get_column_hash("variable_length")},
                    1::numeric as {schema.get_column_hash("variable_order_number")},
                    ''::text as {schema.get_column_hash("variable_format")},
                    1::numeric as {schema.get_column_hash("variable_value_length")}
                WHERE FALSE
            """
            pass

        create_query = f"CREATE UNLOGGED TABLE {schema.hash} AS {view_select}"
        self.data_service.pgi.execute_sql(create_query)
        self.data_service.pgi.schema.add_table(schema)

        return table_name, f"SELECT * FROM {schema.hash}"
