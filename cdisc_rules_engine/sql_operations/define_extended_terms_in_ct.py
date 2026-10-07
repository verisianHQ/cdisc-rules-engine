from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.get_codelist_attributes import SqlGetCodelistAttributesOperation


class SqlDefineExtendedTermsInCtOperation(SqlGetCodelistAttributesOperation):
    """
    Returns the define.xml extended terms that already exist in the library CT version in use,
    in the same codelist, as either:
    - a duplicate of a submission value
    - a synonym of a term
    - a subset of a submission value separated by either "," or ";", i.e. each part of the extended term
      is a part of the submission value, both split by the same separator
      (e.g. "bacteria" in "bacteria, bacteriophage" or in "bacteria; bacteriophage")
    Each result identifies its define codelist, e.g. "Ophthalmic Exam Test Code (C117743): INTP".
    """

    def _execute_operation(self):
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        version_clause, params = self._ct_version_filter("lib.standard_type", "lib.version_date")

        ext_value = self._case("ext.value")
        lib_value = self._case("lib.value")
        synonym = self._case("TRIM(syn.value)")
        ext_part = self._case("TRIM(ext_part.value)")
        lib_part = self._case("TRIM(lib_part.value)")

        where_clauses = [
            "ext.standard_type IS NULL",
            "ext.extensible = 'Yes'",
            "ext.value <> ''",
            "lib.standard_type IS NOT NULL",
            f"""(
                {lib_value} = {ext_value}
                OR EXISTS (
                    SELECT 1 FROM UNNEST(STRING_TO_ARRAY(lib.synonym, ';')) AS syn(value)
                    WHERE {synonym} = {ext_value}
                )
                OR EXISTS (
                    SELECT 1 FROM (VALUES (','), (';')) AS sep(value)
                    WHERE STRPOS(lib.value, sep.value) > 0
                    AND NOT EXISTS (
                        SELECT 1 FROM UNNEST(STRING_TO_ARRAY(ext.value, sep.value)) AS ext_part(value)
                        WHERE {ext_part} NOT IN (
                            SELECT {lib_part} FROM UNNEST(STRING_TO_ARRAY(lib.value, sep.value)) AS lib_part(value)
                        )
                    )
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

    def _case(self, sql: str) -> str:
        return sql if self.params.case_sensitive else f"UPPER({sql})"
