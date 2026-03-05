from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory


class SqlDefineVariablesDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Define Item Metadata Check rules.
    Creates a table containing metadata for the variables extracted from define.xml.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_define_item_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )

        domain = self.dataset_metadata.domain or self.dataset_metadata.name
        define_vars = define_reader.extract_variables_metadata(domain)
        for var in define_vars:
            for k, v in var.items():
                var[k] = ",".join(str(i) for i in v) if isinstance(v, list) else v

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, type in DEFINE_VARIABLES_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)
        self.data_service.pgi.insert_data(table_name, define_vars)

        return table_name
