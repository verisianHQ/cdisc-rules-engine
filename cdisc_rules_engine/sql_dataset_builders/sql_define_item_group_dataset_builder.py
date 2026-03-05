from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory


class SqlDefineItemGroupDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Define Item Group Metadata Check rules.
    Creates a table containing metadata for the domains extracted from define.xml.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_define_item_group"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )

        domain = self.dataset_metadata.domain or self.dataset_metadata.name
        item_groups = define_reader.extract_domain_metadata(domain)

        if not item_groups:
            item_groups = [{}]

        for ig in item_groups:
            for k, v in ig.items():
                if isinstance(v, list):
                    ig[k] = ",".join(v)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        if item_groups and item_groups[0]:
            for key in item_groups[0].keys():
                schema.add_column(SqlColumnSchema.generated(key, "Char"))
        else:
            schema.add_column(SqlColumnSchema.generated("define_dataset_name", "Char"))

        self.data_service.pgi.create_table(schema)

        if item_groups and item_groups[0]:
            self.data_service.pgi.insert_data(table_name, item_groups)

        return table_name
