from typing import Tuple, Optional

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlDatasetMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Dataset Metadata Check rules.
    Creates a table with a single row containing dataset metadata.

    Example table structure:
    dataset_location | dataset_name | dataset_label | record_count
    -----------------|--------------|---------------|-------------
    dm.xpt           | DM           | Demographics  | 100
    """

    def build(self) -> Tuple[str, Optional[str]]:
        """
        Create dataset metadata table and return table name.
        """
        table_name = f"{self.dataset_metadata.name}_dataset_metadata"

        existing_schema = self.data_service.pgi.schema.get_table(table_name)
        if existing_schema is not None:
            return table_name, f"SELECT * FROM {existing_schema.hash}"

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("dataset_location", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("record_count", "Num"))

        self.data_service.pgi.create_table(schema)

        table_hash = self.data_service.pgi.schema.get_table_hash(self.dataset_metadata.name)
        count_query = f"SELECT COUNT(*) as count FROM {table_hash};"
        self.data_service.pgi.execute_sql(count_query)
        count_result = self.data_service.pgi.fetch_all()
        record_count = count_result[0]["count"] if count_result else 0

        row = {
            "dataset_location": self.dataset_metadata.filename,
            "dataset_name": self.dataset_metadata.name,
            "dataset_label": self.dataset_metadata.label or "",
            "record_count": record_count,
        }

        self.data_service.pgi.insert_data(table_name, [row])
        return table_name, f"SELECT * FROM {table_name}"
