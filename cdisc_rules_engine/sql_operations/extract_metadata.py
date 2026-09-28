from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.constants.metadata_columns import DATASET_NAME, SOURCE_DS
from cdisc_rules_engine.constants.metadata_mappings import METADATA_MAPPINGS


class SqlExtractMetadataOperation(SqlBaseOperation):
    SOURCE_DS_PARAM = "$source_ds"

    def _execute_operation(self):
        if self.params.target == DATASET_NAME and self.params.dataset_metadata:
            return self._dataset_name_result()

        domain_metadata = self.params.standards_context.get_domain_metadata(self.params.domain)

        # Map differing attribute names between rules and metadata
        mapped_target = METADATA_MAPPINGS.get(self.params.target, self.params.target)

        final_val = domain_metadata.get(mapped_target)

        if not final_val:
            raise Exception(f"Metadata extraction of {self.params.target} failed - metadata not found")

        return SqlOperationResult(
            query=f"SELECT '{final_val.replace('\'', '\'\'')}' AS value", type="constant", subtype="Char"
        )

    def _dataset_name_result(self) -> SqlOperationResult:
        """
        The name of the dataset being validated (e.g. SUPPLBCH), not the library domain.
        Resolved per record from SOURCE_DS so concatenated split datasets report the
        dataset each record came from.
        """
        fallback = self.params.dataset_metadata.name.upper().replace("'", "''")
        if self.params.table and self.data_service.pgi.schema.column_exists(self.params.table, SOURCE_DS):
            return SqlOperationResult(
                query=f"SELECT COALESCE(NULLIF({self.SOURCE_DS_PARAM}::text, ''), '{fallback}') AS value",
                type="constant",
                subtype="Char",
                params={self.SOURCE_DS_PARAM: SOURCE_DS},
            )
        return SqlOperationResult(query=f"SELECT '{fallback}' AS value", type="constant", subtype="Char")
