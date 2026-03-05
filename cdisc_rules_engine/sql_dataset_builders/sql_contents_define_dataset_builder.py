from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)


class SqlContentsDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Dataset Contents Check against Define XML rules.
    Adds dataset metadata from Define XML to the dataset for use in rules.
    """

    def build(self) -> str:
        table_id = self.data_service.get_dataset_for_rule(self.dataset_metadata, self.rule, self.standards_context)

        schema = self.data_service.pgi.schema.get_table(table_id)
        if not schema:
            raise ValueError(f"Table {table_id} not found")

        for col in ["dataset_location", "dataset_name", "dataset_label", "dataset_domain"]:
            self.data_service.pgi.add_column(table_id, SqlColumnSchema.define(col, "Char"))

        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        define_ds_metadata = define_reader.extract_dataset_metadata(self.dataset_metadata.domain)

        for key in define_ds_metadata:
            self.data_service.pgi.add_column(table_id, SqlColumnSchema.define(f"{key}", "Char"))

        dataset_location = self.dataset_metadata.filename
        dataset_name = self.dataset_metadata.name
        dataset_label = self.dataset_metadata.label
        dataset_domain = self.dataset_metadata.domain

        table_hash = self.data_service.pgi.schema.get_table_hash(table_id)

        sets = {
            "dataset_location": dataset_location,
            "dataset_name": dataset_name,
            "dataset_label": dataset_label,
            "dataset_domain": dataset_domain,
            "define_dataset_location": define_ds_metadata["define_dataset_location"],
            "define_dataset_name": define_ds_metadata["define_dataset_name"],
            "define_dataset_label": define_ds_metadata["define_dataset_label"],
            "define_dataset_domain": define_ds_metadata["define_dataset_domain"],
            "define_dataset_class": define_ds_metadata["define_dataset_class"],
            "define_dataset_structure": define_ds_metadata["define_dataset_structure"],
            "define_dataset_is_non_standard": define_ds_metadata["define_dataset_is_non_standard"],
            "define_dataset_variables": ",".join(define_ds_metadata["define_dataset_variables"]),
            "define_dataset_key_sequence": ",".join(define_ds_metadata["define_dataset_key_sequence"]),
        }

        set_query = ", ".join(
            [
                f"{self.data_service.pgi.schema.get_column_hash(table_id, col)} = '{value}'"
                for col, value in sets.items()
            ]
        )

        update_query = f"""
            UPDATE {table_hash} SET {set_query} WHERE id = 1;
        """
        self.data_service.pgi.execute_sql(update_query)

        return table_id
