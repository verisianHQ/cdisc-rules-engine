from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.get_codelist_attributes import SqlGetCodelistAttributesOperation


class SqlDefineExtendedTermsInCtOperation(SqlGetCodelistAttributesOperation):
    """
    Returns the define.xml extended terms that already exist in the library CT version in use,
    either as a submission value or as a synonym of a term in the same codelist. 
    Each result identifies its define codelist, e.g. "Ophthalmic Exam Test Code (C117743): INTP".
    """

    def _execute_operation(self):
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        version_clause, params = self._ct_version_filter("lib.standard_type", "lib.version_date")

        ext_value = "ext.value" if self.params.case_sensitive else "UPPER(ext.value)"
        lib_value = "lib.value" if self.params.case_sensitive else "UPPER(lib.value)"
        synonym = "TRIM(syn.value)" if self.params.case_sensitive else "UPPER(TRIM(syn.value))"

        where_clauses = [
            "ext.standard_type IS NULL",
            "ext.extensible = 'Yes'",
            "lib.standard_type IS NOT NULL",
            f"""(
                {lib_value} = {ext_value}
                OR EXISTS (
                    SELECT 1 FROM UNNEST(STRING_TO_ARRAY(lib.synonym, ';')) AS syn(value)
                    WHERE {synonym} = {ext_value}
                )
            )""",
        ]
        if version_clause:
            where_clauses.append(version_clause)

        query = f"""
            SELECT DISTINCT CONCAT(ext.name, ' (', ext.codelist_code, '): ', ext.value) AS value
            FROM {ct_table} ext
            JOIN {ct_table} lib ON lib.codelist_code = ext.codelist_code
            WHERE {' AND '.join(where_clauses)}
            ORDER BY value
        """

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=params or None)
