import json
from typing import Dict, List

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlReferencedVariableMetadata(SqlBaseOperation):
    """
    Looks up library variable metadata by a key (label or name) held in each record's target column.
    The library is inlined as one jsonb object {key: [values]}: Postgres parses it once per query and
    each row is a keyed lookup, not a scan of every library variable.
    """

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, key_attribute: str):
        super().__init__(params, data_service)
        self.key_attribute = key_attribute

    def _execute_operation(self):
        attribute = getattr(self.params, "attribute_name", "role")
        mapping = self._library_mapping(attribute)
        if not mapping:
            return SqlOperationResult(query="SELECT NULL", type="constant", subtype="Char")

        literal = json.dumps(mapping).replace("'", "''")
        return SqlOperationResult(
            query=f"SELECT jsonb_array_elements_text('{literal}'::jsonb -> %target%::text) AS value",
            type="collection",
            subtype="Char",
            params={"%target%": self.params.target},
        )

    def _library_mapping(self, attribute: str) -> Dict[str, List[str]]:
        standard_metadata = self.params.standards_context.get_standard_metadata()
        mapping: Dict[str, List[str]] = {}
        for cls in standard_metadata.get("classes", []):
            for dataset in cls.get("datasets", []):
                for var in dataset.get("datasetVariables", []):
                    key = var.get(self.key_attribute)
                    value = var.get(attribute)
                    if not key or value is None:
                        continue
                    values = mapping.setdefault(key, [])
                    if str(value) not in values:
                        values.append(str(value))
        return mapping
