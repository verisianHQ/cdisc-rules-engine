from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
    LIBRARY_VARIABLES_TYPE,
)


class SqlDefineItemMetadataCheckAgainstLibraryDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Define Item Metadata Check against Library rules.
    Creates a table containing metadata for the variables extracted from define.xml and library metadata.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_define_item_metadata_library"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        lib_key_map = {key: key.replace("library_variable_", "") for key in LIBRARY_VARIABLES_TYPE.keys()}
        def_keys = list(DEFINE_VARIABLES_TYPE.keys())

        define_vars = self.get_define_vars()
        define_vars_by_name = {var["define_variable_name"].upper(): var for var in define_vars}

        library_metadata = self.standards_context.get_library_variables_metadata(self.dataset_metadata)
        self.flatten_lists_in_dict(library_metadata)
        library_vars_by_name = {var["name"].upper(): var for var in library_metadata}

        all_var_names = set(define_vars_by_name.keys()) | set(library_vars_by_name.keys())

        rows = []
        for var_name in all_var_names:
            define_var = define_vars_by_name.get(var_name, {})
            library_var = library_vars_by_name.get(var_name, {})

            row = {}

            for target_key, source_key in lib_key_map.items():
                val = library_var.get(source_key)
                row[target_key] = None if val == "" else val

            for key in def_keys:
                val = define_var.get(key)
                row[key] = None if val == "" else val

            rows.append(row)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, type in list(DEFINE_VARIABLES_TYPE.items()) + list(LIBRARY_VARIABLES_TYPE.items()):
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
