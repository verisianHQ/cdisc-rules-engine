from cdisc_rules_engine.constants.metadata_columns import METADATA_COLUMNS
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlReferencedDomainVariableNamesOperation(SqlBaseOperation):
    """
    Returns the variable (column) names of whichever dataset in the study matches the
    domain code found in self.params.target (e.g. RDOMAIN) on the row under evaluation.
    """

    def _execute_operation(self):
        rows = []
        seen_domains = set()
        for dataset in self.data_service.datasets:
            domain = (dataset.domain or "").upper()
            if not domain or domain in seen_domains:
                continue
            table = self.data_service.pgi.schema.get_table(dataset.name)
            if table is None:
                continue
            seen_domains.add(domain)
            escaped_domain = domain.replace("'", "''")
            for _, column in table.get_columns():
                if column.name == "id" or column.name in METADATA_COLUMNS:
                    continue
                escaped_variable = column.name.upper().replace("'", "''")
                rows.append(f"('{escaped_domain}', '{escaped_variable}')")

        if not rows:
            query = "SELECT value FROM (VALUES (NULL, NULL)) AS domain_vars(domain, value) WHERE FALSE"
        else:
            values_clause = ", ".join(rows)
            query = (
                f"SELECT DISTINCT value FROM (VALUES {values_clause}) AS domain_vars(domain, value) "
                "WHERE domain = $1"
            )

        return SqlOperationResult(
            query=query,
            type="collection",
            subtype="Char",
            params={"$1": self.params.target},
        )
