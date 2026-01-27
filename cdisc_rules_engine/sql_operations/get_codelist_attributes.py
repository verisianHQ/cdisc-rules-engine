from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

_column_map = {
    "Term CCODE": "item_code",
    "Term Signification": "value",
    "Codelist Code": "codelist_code",
    "Codelist Name": "name",
    "Term": "term",
}


class SqlGetCodelistAttributesOperation(SqlBaseOperation):
    """
    Retrieves a list of codelist attributes (e.g. Term CCODEs) for a specific
    standard version defined in a dataset column.
    """

    def _execute_operation(self):
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        attribute = self.params.ct_attribute
        version = self.params.ct_version
        domain = self.params.domain

        select_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, _column_map.get(attribute, "item_code"))
        version_date_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "version_date")
        std_type_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "standard_type")

        ct_packages = self.params.standards_context.get_ct_packages()
        ct_types = {ct.split("ct")[0] for ct in ct_packages}
        types_list_sql = ", ".join([f"'{t}'" for t in ct_types])

        op_params = {}
        if self.data_service.pgi.schema.column_exists(domain, version):
            # version is a column name
            version_val_sql = "$1"
            op_params[version_val_sql] = version.lower()
        else:
            # version is a date string literal
            version_val_sql = f"'{version}'"

        query = f"""
            SELECT DISTINCT {select_col_sql} AS value
            FROM {ct_table}
            WHERE {std_type_col_sql} IN ({types_list_sql})
              AND {version_date_col_sql} = {version_val_sql}
        """

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=op_params)
