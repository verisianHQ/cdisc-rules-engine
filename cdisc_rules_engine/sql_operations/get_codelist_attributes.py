import re
import warnings
from typing import Optional

from cdisc_rules_engine.data_service.startup.populate_codelists import ROOT_PATH, populate_codelists
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.standards.adam_standards_context import AdamStandardsContext

_VERSION_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
# reference terminology values meaning CDISC CT, as in the non-SQL get_codelist_attributes operation
_CDISC_CT_REFERENCES = ("CDISC", "CDISC CT")

_COLUMN_MAP = {
    "Standard Type": "standard_type",
    "Version Date": "version_date",
    "Term CCODE": "item_code",
    "Codelist Code": "codelist_code",
    "Extensible": "extensible",
    "Codelist Name": "name",
    "Term Signification": "value",
    "Synonyms": "synonym",
    "Definition": "definition",
    "Term": "term",
    "Standard and Date": "standard_and_date",
}


class SqlGetCodelistAttributesOperation(SqlBaseOperation):
    """
    Retrieves a list of codelist attributes (e.g. Term CCODEs) for a specific
    standard version defined in a dataset column.
    """

    def _execute_operation(self):
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        attribute = self.params.ct_attribute

        # CT version precedence:
        # - the version in an operation-referenced dataset column (e.g. TSVCDVER), where populated
        # - the provided codelists (Library sheet / -ct), or else a literal version given in the operation
        # - otherwise all loaded CT packages (the most recent cached one being loaded too)
        ct_version_column = self._ct_version_column()
        fallback_versions = self.data_service.provided_codelists or (
            None if ct_version_column else self.params.ct_version
        )

        if ct_version_column:
            self._load_record_ct_packages(ct_version_column)
        if not ct_version_column and not fallback_versions:
            self._load_latest_ct_package()

        raw_col = self.data_service.pgi.schema.get_column_hash(ct_table, _COLUMN_MAP.get(attribute, "item_code"))

        if attribute == "Synonym":
            select_col_sql = f"TRIM(UNNEST(STRING_TO_ARRAY({raw_col}, ';')))"
        else:
            select_col_sql = raw_col

        version_date_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "version_date")
        std_type_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "standard_type")

        where_clauses = []
        params = {}

        version_clause = self._version_clause(
            ct_version_column, fallback_versions, std_type_col_sql, version_date_col_sql
        )
        if version_clause:
            where_clauses.append(version_clause)
        if ct_version_column:
            params["$ct_version"] = ct_version_column

        conditions = self.params.ct_conditions
        if conditions:
            for condition in conditions:
                for k, v in condition.items():
                    where_clauses.append(
                        f"{_COLUMN_MAP.get(k)} = '{v}'" if v is not None else f"{_COLUMN_MAP.get(k)} IS NULL"
                    )

        base_query = f"""
            SELECT DISTINCT {select_col_sql} AS value
            FROM {ct_table}
        """

        if where_clauses:
            query = f"{base_query} WHERE {' AND '.join(where_clauses)}"
        else:
            query = base_query

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=params or None)

    def _ct_version_column(self) -> Optional[str]:
        ct_version = self.params.ct_version
        if not ct_version or not isinstance(ct_version, str) or not self.params.table:
            return None
        if not self.data_service.pgi.schema.column_exists(self.params.table, ct_version):
            return None
        return ct_version

    def _version_clause(
        self, ct_version_column: Optional[str], fallback_versions, std_type_col: str, version_date_col: str
    ) -> Optional[str]:
        fallback_clause = None
        if fallback_versions:
            ct_list = sorted(fallback_versions) if isinstance(fallback_versions, (list, set)) else [fallback_versions]
            fallback_clause = self._build_clauses(self._parse_versions(ct_list), std_type_col, version_date_col)
        if not ct_version_column:
            return fallback_clause
        record_clause = f"{version_date_col} = $ct_version"
        if not fallback_clause:
            return record_clause
        return f"(CASE WHEN NULLIF($ct_version, '') IS NOT NULL THEN {record_clause} ELSE {fallback_clause} END)"

    def _ct_standard_type(self) -> str:
        return "adam" if isinstance(self.params.standards_context, AdamStandardsContext) else "sdtm"

    def _load_record_ct_packages(self, ct_version_column: str):
        """Loads the cached CT packages named by the record-level version column, if not loaded yet."""
        pgi = self.data_service.pgi
        table_hash = pgi.schema.get_table_hash(self.params.table)
        column_hash = pgi.schema.get_column_hash(self.params.table, ct_version_column)
        query = f"SELECT DISTINCT {column_hash} AS version FROM {table_hash}"
        reference_hash = (
            pgi.schema.get_column_hash(self.params.table, self.params.target) if self.params.target else None
        )
        if reference_hash:
            query += f" WHERE {reference_hash} IN ({', '.join(repr(ref) for ref in _CDISC_CT_REFERENCES)})"
        pgi.execute_sql(query)
        versions = {row["version"] for row in pgi.fetch_all() if _VERSION_DATE_PATTERN.fullmatch(row["version"] or "")}
        self._load_ct_packages(versions, f"column {ct_version_column}")

    def _load_latest_ct_package(self):
        """Loads the most recent cached CT package, if not loaded yet."""
        if not self.data_service.cache_path:
            return
        prefix = f"{self._ct_standard_type()}ct-"
        cached = sorted(
            path.stem[len(prefix) :]
            for path in (ROOT_PATH / self.data_service.cache_path).glob(f"{prefix}*.pkl")
            if _VERSION_DATE_PATTERN.fullmatch(path.stem[len(prefix) :])
        )
        if cached:
            self._load_ct_packages({cached[-1]}, "default (most recent) CT version")

    def _load_ct_packages(self, versions: set, referenced_by: str):
        """Loads the cached CT packages for the given versions that are not loaded yet, warning about missing ones."""
        cache_path = self.data_service.cache_path
        if not cache_path or not versions:
            return
        pgi = self.data_service.pgi
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        ct_type = self._ct_standard_type()
        if pgi.schema.get_table(ct_table):
            pgi.execute_sql(f"SELECT DISTINCT version_date FROM {ct_table} WHERE standard_type = '{ct_type}'")
            versions = versions - {row["version_date"] for row in pgi.fetch_all()}

        to_load = []
        for version in sorted(versions):
            file_name = f"{ct_type}ct-{version}.pkl"
            if (ROOT_PATH / cache_path / file_name).is_file():
                to_load.append(file_name)
            else:
                warnings.warn(
                    f"CT package {ct_type}ct-{version} referenced by {referenced_by} is not available in the cache "
                    f"({cache_path}): records referencing it cannot be checked against it"
                )
        populate_codelists(pgi, cache_path, to_load)

    def _parse_versions(self, ct_list: list) -> list:
        provided_cts = []
        for ct in ct_list:
            if isinstance(ct, str):
                if "ct-" in ct:
                    parts = ct.split("ct-")
                    provided_cts.append({"type": parts[0], "version": parts[1]})
                else:
                    provided_cts.append({"type": None, "version": ct})
        return provided_cts

    def _build_clauses(self, provided_cts: list, std_type_col: str, version_date_col: str) -> str:
        or_conditions = []
        for ct in provided_cts:
            and_conditions = []
            if ct["type"]:
                and_conditions.append(f"{std_type_col} = '{ct['type']}'")
            if ct["version"]:
                and_conditions.append(f"{version_date_col} = '{ct['version']}'")
            if and_conditions:
                or_conditions.append(f"({' AND '.join(and_conditions)})")

        return f"({' OR '.join(or_conditions)})" if or_conditions else "TRUE"
