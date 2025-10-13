from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlVariablesMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Variable Metadata Check rules.
    Creates a table with variable metadata for the specific dataset.
    Queries data_metadata table to get variable metadata (labels, formats, types).

    Example table structure (one row per variable):
       variable_name | variable_order_number | variable_label | variable_size | variable_data_type | variable_format
    -----------------|----------------------|----------------|---------------|-------------------|----------------
       STUDYID       | 1                    | Study ID       | 16            | text              | $16.
       USUBJID       | 2                    | Subject ID     | 20            | text              | $20.
       AETERM        | 3                    | AE Term        | 200           | text              | $200.
    """

    def build(self) -> str:
        """
        Create variable metadata table for this dataset and return table name.
        """
        table_name = f"{self.dataset_metadata.dataset_id}_var_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        # Create table schema with required columns
        schema = SqlTableSchema.static(table_name)
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))

        self.data_service.pgi.create_table(schema)

        rows = []

        for variable in self.dataset_metadata.variables:
            rows.append(
                {
                    "variable_name": variable.name.upper(),
                    "variable_order_number": variable.order,
                    "variable_label": variable.label,
                    "variable_size": variable.length,
                    "variable_data_type": variable.type,
                    "variable_format": variable.format,
                }
            )

        self.data_service.pgi.insert_data(table_name, rows)
        return table_name
