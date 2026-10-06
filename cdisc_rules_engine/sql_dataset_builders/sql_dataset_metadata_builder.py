from cdisc_rules_engine.constants.metadata_columns import SOURCE_DS
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
    dataset_location | dataset_name | dataset_label | record_count | dataset_size
    -----------------|--------------|---------------|--------------|-------------
    dm.xpt           | DM           | Demographics  | 100          | 81920
    """

    def build(self) -> str:
        """
        Create dataset metadata table and return table name.
        """
        table_name = f"{self.dataset_metadata.name}_dataset_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        split_parts = getattr(self.dataset_metadata, "split_part_filenames", None)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("dataset_location", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("record_count", "Num"))
        schema.add_column(SqlColumnSchema.generated("dataset_size", "Num"))
        if split_parts:
            schema.add_column(SqlColumnSchema.generated(SOURCE_DS, "Char"))

        self.data_service.pgi.create_table(schema)

        if split_parts:
            rows = self._split_part_rows(split_parts)
        else:
            table_hash = self.data_service.pgi.schema.get_table_hash(self.dataset_metadata.name)
            count_query = f"SELECT COUNT(*) as count FROM {table_hash};"
            self.data_service.pgi.execute_sql(count_query)
            count_result = self.data_service.pgi.fetch_all()
            record_count = count_result[0]["count"] if count_result else 0

            rows = [
                {
                    "dataset_location": self.dataset_metadata.filename,
                    "dataset_name": self.dataset_metadata.name,
                    "dataset_label": self.dataset_metadata.label or "",
                    "record_count": record_count,
                    "dataset_size": self.dataset_metadata.file_size,
                }
            ]

        self.data_service.pgi.insert_data(table_name, rows)
        return table_name

    def _split_part_rows(self, split_parts: list) -> list:
        table_hash = self.data_service.pgi.schema.get_table_hash(self.dataset_metadata.name)
        source_ds_hash = self.data_service.pgi.schema.get_column_hash(self.dataset_metadata.name, SOURCE_DS)
        self.data_service.pgi.execute_sql(
            f"SELECT UPPER({source_ds_hash}) as source_ds, COUNT(*) as count "
            f"FROM {table_hash} GROUP BY UPPER({source_ds_hash});"
        )
        counts = {row["source_ds"]: row["count"] for row in self.data_service.pgi.fetch_all()}
        labels = getattr(self.dataset_metadata, "split_part_labels", None) or {}
        sizes = getattr(self.dataset_metadata, "split_part_sizes", None) or {}

        rows = []
        for filename in sorted(split_parts):
            part_name = filename.rsplit(".", 1)[0].upper()
            rows.append(
                {
                    "dataset_location": filename,
                    "dataset_name": part_name,
                    "dataset_label": labels.get(filename, self.dataset_metadata.label) or "",
                    "record_count": counts.get(part_name, 0),
                    "dataset_size": sizes.get(filename),
                    SOURCE_DS: part_name,
                }
            )
        return rows
