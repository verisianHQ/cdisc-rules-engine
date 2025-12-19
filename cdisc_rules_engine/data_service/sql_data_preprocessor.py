"""
Data Preprocessor for SDTM and ADaM clinical data.
"""

from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from cdisc_rules_engine.data_service.postgresql_data_service import (
        PostgresQLDataService,
    )
    from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER, SOURCE_DS
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2, VariableMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services import logger


class SqlDataPreprocessor:
    """
    Performs preprocessing operations on clinical data.
    Operations should be performed at data ingestion time.
    """

    def __init__(self, data_service: "PostgresQLDataService", standards_context: "BaseStandardsContext"):
        self.data_service = data_service
        self.standards_context = standards_context

    def _get_table_hash(self, table_name: str) -> str:
        table_hash = self.data_service.pgi.schema.get_table_hash(table_name)
        if table_hash:
            return table_hash
        return table_name.lower()

    def preprocess_all(self) -> None:
        """Execute preprocessing stages."""
        logger.info("Starting data preprocessing pipeline")

        self._process_split_datasets()
        self._update_metadata(datetime.now().astimezone())

        logger.info("Data preprocessing pipeline completed")

    def _process_split_datasets(self) -> None:
        """Concatenate split datasets into single logical datasets."""
        logger.info("Processing split datasets")

        dataset_names = [ds.name.lower() for ds in self.data_service.datasets]
        split_groups = self.standards_context.detect_split_datasets(dataset_names)

        if not split_groups:
            logger.info("No split datasets found")
            return

        for unsplit_name, dataset_parts in split_groups.items():
            logger.info(f"Concatenating {len(dataset_parts)} parts for {unsplit_name}: {', '.join(dataset_parts)}")
            self._concatenate_split_parts(unsplit_name, dataset_parts)

    def _concatenate_split_parts(self, unsplit_name: str, dataset_parts: List[str]) -> None:
        """Concatenate multiple dataset parts into a single table."""
        if not dataset_parts:
            logger.warning(f"No parts to concatenate for {unsplit_name}")
            return

        first_part_schema = self.data_service.pgi.schema.get_table(dataset_parts[0])
        if not first_part_schema:
            logger.error(f"Schema not found for first part: {dataset_parts[0]}")
            return

        source_ds_hash = first_part_schema.get_column_hash(SOURCE_DS) or SOURCE_DS
        source_row_hash = first_part_schema.get_column_hash(SOURCE_ROW_NUMBER) or SOURCE_ROW_NUMBER

        union_parts = []
        for part in dataset_parts:
            part_hash = self._get_table_hash(part)
            union_parts.append(f"SELECT * FROM public.{part_hash}")

        union_query = " UNION ALL ".join(union_parts)

        unsplit_schema = SqlTableSchema.from_join(unsplit_name, self.data_service.pgi)

        for col_name, col_schema in first_part_schema.get_columns():
            if col_name.lower() != "id":
                unsplit_schema.add_column(col_schema)

        self.data_service.pgi.create_table(unsplit_schema)

        unsplit_hash = unsplit_schema.hash

        columns = [
            col_schema.hash
            for col_name, col_schema in unsplit_schema.get_columns()
            if col_name.lower() != "id" and not col_schema.alias
        ]
        columns_str = ", ".join(columns)

        insert_query = f"""
            INSERT INTO public.{unsplit_hash} ({columns_str})
            SELECT {columns_str} FROM (
                {union_query}
            ) AS concatenated
            ORDER BY {source_ds_hash}, {source_row_hash}
        """

        self.data_service.pgi.execute_sql(insert_query)

        index_queries = [
            f"CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_source_ds " f"ON public.{unsplit_hash}({source_ds_hash})",
            f"CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_source_row " f"ON public.{unsplit_hash}({source_row_hash})",
        ]

        studyid_hash = unsplit_schema.get_column_hash("studyid")
        usubjid_hash = unsplit_schema.get_column_hash("usubjid")

        if studyid_hash and usubjid_hash:
            index_queries.append(
                f"CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_studyid_usubjid "
                f"ON public.{unsplit_hash}({studyid_hash}, {usubjid_hash})"
            )

        for idx_query in index_queries:
            self.data_service.pgi.execute_sql(idx_query)

        logger.info(f"Concatenated dataset: {unsplit_name}")

    def _create_metadata_from_split_parts(
        self, unsplit_name: str, source_parts: List[str]
    ) -> Optional[DatasetMetadata2]:
        """Create metadata object by merging split part metadata."""
        part_metadata = []
        for part_name in source_parts:
            part_meta = next((ds for ds in self.data_service.datasets if ds.name.lower() == part_name.lower()), None)
            if part_meta:
                part_metadata.append(part_meta)

        if not part_metadata:
            return None

        first_part = part_metadata[0]
        merged_variables = []
        seen_vars = set()

        for part in part_metadata:
            for var in part.variables:
                var_name = var.name.upper()
                if var_name not in seen_vars:
                    merged_variables.append(
                        VariableMetadata(
                            name=var_name,
                            label=var.label,
                            type=var.type,
                            length=var.length,
                            format=var.format,
                            order=var.order,
                        )
                    )
                    seen_vars.add(var_name)

        metadata = deepcopy(first_part)
        file_type = first_part.filename.split(".")[-1].lower()
        metadata.filename = f"{unsplit_name}.{file_type}"
        metadata.name = unsplit_name.upper()
        metadata.variables = merged_variables

        return metadata

    def _update_metadata(self, timestamp: datetime) -> None:
        """Update metadata for preprocessed datasets."""
        split_update_query = """
            UPDATE public.data_metadata
            SET
                dataset_preprocessed = %s,
                preprocessing_stage = 'split_processed',
                updated_at = %s
            WHERE dataset_is_split = true
        """
        self.data_service.pgi.execute_sql(split_update_query, (timestamp, timestamp))

    @staticmethod
    def run(data_service: "PostgresQLDataService", standards_context: "BaseStandardsContext") -> None:
        preprocessor = SqlDataPreprocessor(data_service, standards_context)
        preprocessor.preprocess_all()
