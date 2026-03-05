from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory


class SqlDatasetMetadataWithDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Creates a table merging the physical dataset metadata with Define-XML dataset metadata.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_ds_metadata_with_define"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        all_ds_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        define_ds_metadata = {
            ds_metadata.domain: define_reader.extract_dataset_metadata(ds_metadata.domain)
            for ds_metadata in all_ds_metadata
        }

        rows = []
        all_keys = set(
            [
                "dataset_name",
                "dataset_location",
                "dataset_label",
                "dataset_domain",
            ]
        )
        for domain in define_ds_metadata:
            all_keys.update(list(define_ds_metadata[domain].keys()))

        for ds_metadata in all_ds_metadata:
            define_metadata = define_ds_metadata.get(ds_metadata.domain, {})
            row_data = {
                "dataset_name": ds_metadata.name,
                "dataset_location": ds_metadata.filename,
                "dataset_label": ds_metadata.label or "",
                "dataset_domain": ds_metadata.domain,
            }
            for key in define_metadata:
                row_data[key] = define_metadata[key]
            rows.append(row_data)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for key in all_keys:
            col_type = "Num" if any(keyword in key.lower() for keyword in ["number", "size"]) else "Char"
            schema.add_column(SqlColumnSchema.generated(key, col_type))

        self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
