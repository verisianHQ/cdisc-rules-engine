import os
from pathlib import Path
from typing import List, Optional

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER, SOURCE_DS
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.models.sql.table_schema import SqlColumnSchema, SqlTableSchema
from cdisc_rules_engine.readers.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.ingestion_progress import (
    DisabledIngestionProgressReporter,
    IngestionProgressReporter,
)


class SqlDatasetLoader:

    @staticmethod
    def load_datasets(
        pgi: PostgresQLInterface,
        dataset_paths: List[str],
        progress_reporter: Optional[IngestionProgressReporter] = None,
    ) -> List[DatasetMetadata2]:
        """
        Iterate through dataset files in `self.dataset_paths`
        and create corresponding SQL tables.
        """
        reporter = progress_reporter or DisabledIngestionProgressReporter()
        total_bytes = sum(os.path.getsize(path) for path in dataset_paths if os.path.exists(path))
        reporter.start(total_files=len(dataset_paths), total_bytes=total_bytes)

        try:
            return [
                SqlDatasetLoader._load_dataset_file(
                    pgi,
                    file_index,
                    len(dataset_paths),
                    file_path,
                    reporter,
                )
                for file_index, file_path in enumerate(dataset_paths)
            ]
        finally:
            reporter.finish()

    @staticmethod
    def _load_dataset_file(
        pgi: PostgresQLInterface,
        file_index: int,
        total_files: int,
        file_path_str: str,
        reporter: IngestionProgressReporter,
    ) -> DatasetMetadata2:
        """Load a single dataset file."""
        file_path = Path(file_path_str)
        try:
            reader = DataReaderFactory.get_data_reader(file_path_str)
            metadata, chunk_stream = reader.read()
            total_rows = reader._get_total_rows()

            # force table_name to be lowercase
            table_name = file_path.stem.lower()

            logger.info(f"Loading dataset {file_path.name} into table {table_name}")

            schema = SqlTableSchema.from_metadata(metadata, pgi)
            source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
            schema.add_column(source_row_column)

            source_ds_column = SqlColumnSchema(name=SOURCE_DS, hash=SOURCE_DS, type="Char")
            schema.add_column(source_ds_column)

            pgi.create_table(schema)
            # TODO: INDEX

            reporter.start_file(
                file_index=file_index,
                total_files=total_files,
                file_name=file_path.name,
                file_bytes=os.path.getsize(file_path_str) if os.path.exists(file_path_str) else 0,
                total_rows=total_rows,
            )

            row_number = 0

            for chunk_data in chunk_stream:
                # force lowercase on columns
                chunk_data = [{k.lower(): v for k, v in row.items()} for row in chunk_data]

                if chunk_data and SOURCE_ROW_NUMBER in chunk_data[0]:
                    raise ValueError(
                        f"Dataset file '{file_path.name}' contains reserved column '{SOURCE_ROW_NUMBER}'. "
                        "This column is automatically generated and should not be in source data."
                    )

                if chunk_data and SOURCE_DS in chunk_data[0]:
                    raise ValueError(
                        f"Dataset file '{file_path.name}' contains reserved column '{SOURCE_DS}'. "
                        "This column is automatically generated and should not be in source data."
                    )

                for row in chunk_data:
                    row_number += 1
                    row[SOURCE_ROW_NUMBER] = row_number
                    row[SOURCE_DS] = table_name.upper()
                pgi.insert_data(table_name, chunk_data)
                reporter.report_chunk(len(chunk_data))

            reporter.end_file(row_number)

            logger.info(f"Successfully loaded {file_path.name}")

            return metadata
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")
            raise
