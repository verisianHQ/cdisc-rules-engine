from typing import List, Optional, Set, Tuple

from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

_NO_MATCH_VALUES = "(NULL::text, NULL::text)"


class SqlReferencedVariableMetadata(SqlBaseOperation):
    """
    Looks up library variable metadata by a key (label or name) held in each record's target column.
    """

    key_attribute: str
    key_alias: str

    def _execute_operation(self):
        attribute = getattr(self.params, "attribute_name", "role")
        pairs = self._library_pairs(attribute)
        if not pairs:
            return SqlOperationResult(query="SELECT NULL", type="constant", subtype="Char")

        present = self._distinct_target_values()
        if present is not None:
            pairs = [pair for pair in pairs if pair[0] in present]

        values_clause = ", ".join(f"({_quote(key)}, {_quote(value)})" for key, value in pairs) or _NO_MATCH_VALUES
        query = (
            f"SELECT value FROM (VALUES {values_clause}) AS t({self.key_alias}, value) "
            f"WHERE t.{self.key_alias} = %target%"
        )

        return SqlOperationResult(
            query=query,
            type="collection",
            subtype="Char",
            params={"%target%": self.params.target},
        )

    def _library_pairs(self, attribute: str) -> List[Tuple[str, str]]:
        standard_metadata = self.params.standards_context.get_standard_metadata()
        pairs = {}
        for cls in standard_metadata.get("classes", []):
            for dataset in cls.get("datasets", []):
                for var in dataset.get("datasetVariables", []):
                    key = var.get(self.key_attribute)
                    value = var.get(attribute)
                    if not key or value is None:
                        continue
                    pairs[(key, str(value))] = None
        return list(pairs)

    def _distinct_target_values(self) -> Optional[Set[str]]:
        """
        Distinct values of the target column, or None when they can't be read
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
