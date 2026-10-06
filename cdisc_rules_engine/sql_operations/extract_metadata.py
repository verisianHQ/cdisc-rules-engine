from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.constants.metadata_columns import DATASET_NAME, DATASET_SIZE, SOURCE_DS
from cdisc_rules_engine.constants.metadata_mappings import METADATA_MAPPINGS


class SqlExtractMetadataOperation(SqlBaseOperation):
    SOURCE_DS_PARAM = "$source_ds"

    def _execute_operation(self):
        if self.params.target == DATASET_NAME and self.params.dataset_metadata:
            return self._dataset_name_result()
        if self.params.target == DATASET_SIZE and self.params.dataset_metadata:
            return self._dataset_size_result()

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
        The name of the dataset being validated. Per record from SOURCE_DS incase of
        concatenated split datasets.
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

    def _dataset_size_result(self) -> SqlOperationResult:
        fallback = self._size_literal(self.params.dataset_metadata.file_size)
        part_sizes = getattr(self.params.dataset_metadata, "split_part_sizes", None)
        if (
            part_sizes
            and self.params.table
            and self.data_service.pgi.schema.column_exists(self.params.table, SOURCE_DS)
        ):
            cases = " ".join(
                f"WHEN '{filename.rsplit('.', 1)[0].upper().replace('\'', '\'\'')}' THEN {self._size_literal(size)}"
                for filename, size in sorted(part_sizes.items())
            )
            return SqlOperationResult(
                query=(
                    f"SELECT (CASE UPPER({self.SOURCE_DS_PARAM}::text) {cases} ELSE {fallback} END)"
                    "::double precision AS value"
                ),
                type="constant",
                subtype="Num",
                params={self.SOURCE_DS_PARAM: SOURCE_DS},
            )
        return SqlOperationResult(query=f"SELECT {fallback}::double precision AS value", type="constant", subtype="Num")

    @staticmethod
    def _size_literal(size) -> str:
        return "NULL" if size is None else str(int(size))
