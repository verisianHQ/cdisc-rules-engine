from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlVariablesMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Variable Metadata Check rules.
    Creates a view with variable metadata for the specific dataset.
    Mirrors VariablesMetadataDatasetBuilder which builds DataFrame on-the-fly.
    """

    def build(self) -> str:
        """
        Create (or replace) variable metadata view for this dataset and return view name.
        The view queries data_metadata table filtered by dataset_id.

        Returns columns matching Python schema:
        - variable_name
        - variable_order_number
        - variable_label
        - variable_size
        - variable_data_type
        - variable_format
        """
        view_name = f"{self.dataset_metadata.dataset_id}_var_metadata"

        create_view_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
            var_name as variable_name,
            ROW_NUMBER() OVER (ORDER BY var_name) as variable_order_number,
            var_label as variable_label,
            var_length as variable_size,
            var_type as variable_data_type,
            var_format as variable_format
        FROM data_metadata
        WHERE dataset_id = '{self.dataset_metadata.dataset_id}';
        """

        self.data_service.pgi.execute_sql(create_view_sql)
        return view_name
