from typing import Tuple, Optional

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    LIBRARY_VARIABLES_TYPE,
)
from cdisc_rules_engine.enums.static_tables import StaticTables


class SqlValueCheckAgainstLibraryDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    is a row in the new dataset. The library variable metadata corresponding to each row's
    variable is attached.
    """

    def build(self) -> Tuple[str, Optional[str]]:
        table_name = f"{self.dataset_metadata.name}_value_check_library_variables"
        existing_schema = self.data_service.pgi.schema.get_table(table_name)
        if existing_schema is not None:
            return table_name, f"SELECT * FROM {existing_schema.hash}"

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        library_vars = self.get_library_vars()
        library_vars_by_name = {v.get("library_variable_name", "").upper(): v for v in library_vars}

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        for col, type in LIBRARY_VARIABLES_TYPE.items():
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
                l_var = library_vars_by_name.get(col_upper, {})

                row_parts = [f"'{col_upper}'"]
                for key in LIBRARY_VARIABLES_TYPE.keys():
                    type = self.data_service.pgi.schema.get_column(table_name, key).type
                    val = str(l_var.get(key, "")).replace("'", "''").strip()
                    if val.lower() in ["", "none", "null"]:
                        row_parts.append("NULL")
                    else:
                        row_parts.append(f"'{val}'" if type == "Char" else val)

                val_rows.append(f"({', '.join(row_parts)})")

            values_sql = ",\n".join(val_rows)
            target_columns = ["row_number", "variable_name", "variable_value"] + list(LIBRARY_VARIABLES_TYPE.keys())
            columns_clause = ", ".join([schema.get_column_hash(col) for col in target_columns])
            select_cols = ", ".join([f"m.{col}" for col in LIBRARY_VARIABLES_TYPE.keys()])

            insert_query = f"""
                WITH lib_meta AS (
                    SELECT * FROM (VALUES
                        {values_sql}
                    ) AS m(var_name, {", ".join(LIBRARY_VARIABLES_TYPE.keys())})
                )
                INSERT INTO {schema.hash} ({columns_clause})
                SELECT
                    CAST(ROW_NUMBER() OVER () AS TEXT) as row_number,
                    j.key as variable_name,
                    j.value as variable_value,
                    {select_cols}
                FROM {source_table_hash} t,
                LATERAL jsonb_each_text({json_build_str}) AS j(key, value)
                LEFT JOIN lib_meta m ON j.key = m.var_name;
            """
            self.data_service.pgi.execute_sql(insert_query)

        self.data_service.pgi.add_column(table_name, SqlColumnSchema.define("library_variable_ccode_values", "Char"))
        self.data_service.pgi.add_column(table_name, SqlColumnSchema.define("library_variable_codelist_name", "Char"))
        ccode_vals_col_hash = self.data_service.pgi.schema.get_column_hash(table_name, "library_variable_ccode_values")
        codelist_name_col_hash = self.data_service.pgi.schema.get_column_hash(
            table_name, "library_variable_codelist_name"
        )
        ccode_col_hash = schema.get_column_hash("library_variable_ccode")
        codelist_name_col_hash = schema.get_column_hash("library_variable_codelist_name")

        codelist_query = f"""
            UPDATE {schema.hash} t
            SET {ccode_vals_col_hash} = sub.library_variable_ccode_values
            FROM (
                WITH t1 AS (SELECT codelist_code, ARRAY_AGG(value) AS library_variable_ccode_values
                FROM {StaticTables.IG_CODELIST_TABLE_NAME.value}
                WHERE codelist_code <> ''
                GROUP BY codelist_code)
                SELECT a.*, b.library_variable_ccode_values
                FROM {schema.hash} a
                JOIN t1 b ON a.{ccode_col_hash} = b.codelist_code
            ) sub
            WHERE t.id = sub.id;
        """

        self.data_service.pgi.execute_sql(codelist_query)

        codelist_name_query = f"""
            UPDATE {schema.hash} t
            SET {codelist_name_col_hash} = sub.library_variable_codelist_name
            FROM (
                SELECT item_code, name as library_variable_codelist_name
                FROM {StaticTables.IG_CODELIST_TABLE_NAME.value}
            ) sub
            WHERE t.{ccode_col_hash} = sub.item_code;
        """

        self.data_service.pgi.execute_sql(codelist_name_query)

        return table_name, f"SELECT * FROM {table_name}"
