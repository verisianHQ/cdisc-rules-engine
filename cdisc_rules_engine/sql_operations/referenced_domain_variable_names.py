import json
from typing import Dict, List

from cdisc_rules_engine.constants.metadata_columns import METADATA_COLUMNS
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlReferencedDomainVariableNamesOperation(SqlBaseOperation):
    """
    Returns the variable (column) names of whichever dataset in the study matches the
    domain code found in self.params.target (e.g. RDOMAIN) on the row under evaluation.
    The study's variables are inlined as one jsonb object {domain: [variables]}:
    Postgres parses it once per query and each row is a keyed lookup.
    """

    def _execute_operation(self):
        literal = json.dumps(self._domain_variables()).replace("'", "''")
        return SqlOperationResult(
            query=f"SELECT jsonb_array_elements_text('{literal}'::jsonb -> ($1)::text) AS value",
            type="collection",
            subtype="Char",
            params={"$1": self.params.target},
        )

    def _domain_variables(self) -> Dict[str, List[str]]:
        """Variable names of the first dataset found for each domain in the study."""
        mapping: Dict[str, List[str]] = {}
        for dataset in self.data_service.datasets:
            domain = (dataset.domain or "").upper()
            if not domain or domain in mapping:
                continue
            table = self.data_service.pgi.schema.get_table(dataset.name)
            if table is None:
                continue
            variables = mapping[domain] = []
            for _, column in table.get_columns():
                if column.name == "id" or column.name in METADATA_COLUMNS:
                    continue
                variable = column.name.upper()
                if variable not in variables:
                    variables.append(variable)
        return mapping
