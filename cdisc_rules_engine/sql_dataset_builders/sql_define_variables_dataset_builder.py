from typing import Tuple, Optional

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
)


class SqlDefineVariablesDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Define Item Metadata Check rules.
    Creates a table containing metadata for the variables extracted from define.xml.
    """

    def build(self) -> Tuple[str, Optional[str]]:
        table_name = f"{self.dataset_metadata.name}_define_item_metadata"
        existing_schema = self.data_service.pgi.schema.get_table(table_name)
        if existing_schema is not None:
            return table_name, f"SELECT * FROM {existing_schema.hash}"

        define_vars = self.get_define_vars()

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, type in DEFINE_VARIABLES_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)
        self.data_service.pgi.insert_data(table_name, define_vars)

        return table_name, f"SELECT * FROM {table_name}"
