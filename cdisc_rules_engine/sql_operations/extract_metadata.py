from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.enums.metadata_mappings import MetadataMappings


class SqlExtractMetadataOperation(SqlBaseOperation):
    def _execute_operation(self):

        dataset_metadata = self._get_full_dataset_metadata()

        target_value = [getattr(dm, MetadataMappings(self.params.target).name) for dm in dataset_metadata]
        final_val = target_value[0] if target_value else ""

        return SqlOperationResult(
            query=f"SELECT '{final_val.replace('\'', '\'\'')}' AS value", type="constant", subtype="Char"
        )

    def _get_full_dataset_metadata(self):
        dataset_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        return dataset_metadata
