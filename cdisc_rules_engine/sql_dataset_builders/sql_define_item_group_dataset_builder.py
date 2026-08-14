from typing import Tuple, Optional

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_DATASETS_TYPE,
)


class SqlDefineItemGroupDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Define Item Group Metadata Check rules.
    Creates a table containing metadata for the domains extracted from define.xml.
    """

    def build(self) -> Tuple[str, Optional[str]]:
        table_name = f"{self.dataset_metadata.name}_define_item_group"

        existing_schema = self.data_service.pgi.schema.get_table(table_name)
        if existing_schema is not None:
            return table_name, f"SELECT * FROM {existing_schema.hash}"

        define_ds_metadata = self.get_define_all_datasets()

        rows = []
        for domain, metadata in define_ds_metadata.items():
            row = {
                "dataset_domain": domain,
            }
            for key, val in metadata.items():
                row[key] = None if val == "" else val
            rows.append(row)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, type in DEFINE_DATASETS_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))
        self.data_service.pgi.create_table(schema)

        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name, f"SELECT * FROM {table_name}"
