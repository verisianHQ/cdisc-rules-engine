from typing import Optional, Set

from cdisc_rules_engine.constants.metadata_columns import METADATA_COLUMNS
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlReferencedDomainVariableNamesOperation(SqlBaseOperation):
    """
    Returns the variable (column) names of whichever dataset in the study matches the
    domain code found in self.params.target (e.g. RDOMAIN) on the row under evaluation.
    """

    def _execute_operation(self):
        referenced = self._referenced_domains()
        pairs = {}
        seen_domains = set()
        for dataset in self.data_service.datasets:
            domain = (dataset.domain or "").upper()
            if not domain or domain in seen_domains:
                continue
            table = self.data_service.pgi.schema.get_table(dataset.name)
            if table is None:
                continue
            seen_domains.add(domain)
            if referenced is not None and domain not in referenced:
                continue
            for _, column in table.get_columns():
                if column.name == "id" or column.name in METADATA_COLUMNS:
                    continue
                pairs[(domain, column.name.upper())] = None

        if not pairs:
            query = "SELECT value FROM (VALUES (NULL, NULL)) AS domain_vars(domain, value) WHERE FALSE"
        else:
            values_clause = ", ".join(f"({_quote(domain)}, {_quote(variable)})" for domain, variable in pairs)
            query = f"SELECT value FROM (VALUES {values_clause}) AS domain_vars(domain, value) WHERE domain = $1"

        return SqlOperationResult(
            query=query,
            type="collection",
            subtype="Char",
            params={"$1": self.params.target},
        )

    def _referenced_domains(self) -> Optional[Set[str]]:
        """
        Distinct values of the target column, or None when they can't be read
        (then every domain is listed and the compiled check behaves as before).
        """
        table = self._table()
        schema = self.data_service.pgi.schema
        table_hash = schema.get_table_hash(table)
        column = schema.get_column(table, self.params.target) if table_hash else None
        if column is None or column.type != "Char":
            return None
        self.data_service.pgi.execute_sql(
            f"SELECT DISTINCT {column.hash} AS value FROM {table_hash} WHERE {column.hash} IS NOT NULL"
        )
        return {row["value"] for row in self.data_service.pgi.fetch_all()}

    def _table(self) -> str:
        dataset_metadata = self.params.dataset_metadata
        if (
            dataset_metadata is not None
            and self.params.domain == dataset_metadata.domain
            and self.data_service.pgi.schema.get_table(self.params.domain) is None
        ):
            return dataset_metadata.name
        return self.params.domain


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
