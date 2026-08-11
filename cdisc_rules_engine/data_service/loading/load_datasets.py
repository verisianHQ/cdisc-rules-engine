from pathlib import Path
from typing import List

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER, SOURCE_DS
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.models.sql.table_schema import SqlColumnSchema, SqlTableSchema
from cdisc_rules_engine.readers.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services import logger


class SqlDatasetLoader:

    @staticmethod
    def load_datasets(pgi: PostgresQLInterface, dataset_paths: List[str]) -> List[DatasetMetadata2]:
        """
        Iterate through dataset files in `self.dataset_paths`
        and create corresponding SQL tables.
        """
        return [SqlDatasetLoader._load_dataset_file(pgi, file_path) for file_path in dataset_paths]

    @staticmethod
    def _load_dataset_file(pgi: PostgresQLInterface, file_path_str: str) -> DatasetMetadata2:
        """Load a single dataset file."""
        file_path = Path(file_path_str)
        try:
            reader = DataReaderFactory.get_data_reader(file_path_str)
            metadata, chunk_stream = reader.read()

            # force table_name to be lowercase
            table_name = file_path.stem.lower()

            logger.info(f"Loading dataset {file_path.name} into table {table_name}")

            schema = SqlTableSchema.from_metadata(metadata, pgi)
            source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
            schema.add_column(source_row_column)

            source_ds_column = SqlColumnSchema(name=SOURCE_DS, hash=SOURCE_DS, type="Char")
            schema.add_column(source_ds_column)

            pgi.create_table(schema)

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

                pgi.insert_data(table_name, chunk_data)

            SqlDatasetLoader._finalise_loaded_table(pgi, schema, table_name)

            logger.info(f"Successfully loaded {file_path.name}")

            return metadata
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")
            raise

    @staticmethod
    def _finalise_loaded_table(pgi: PostgresQLInterface, schema: SqlTableSchema, table_name: str) -> None:
        """
        Populate generated metadata columns and create indexes after bulk loading.

        source_row_number and source_ds are populated in a single UPDATE rather than
        row-by-row in Python. Indexes are then created on the columns most commonly
        used in joins and filters.
        """
        table_hash = schema.hash
        source_ds_upper = table_name.upper()

        update_query = f"""
            UPDATE {table_hash} AS t
            SET
                {SOURCE_ROW_NUMBER} = sub.rn,
                {SOURCE_DS} = %s
            FROM (
                SELECT id, row_number() OVER (ORDER BY id) AS rn
                FROM {table_hash}
            ) AS sub
            WHERE t.id = sub.id
        """
        pgi.execute_sql(update_query, (source_ds_upper,))

        index_queries = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_source_row ON {table_hash}({SOURCE_ROW_NUMBER})",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_source_ds ON {table_hash}({SOURCE_DS})",
        ]

        studyid_hash = schema.get_column_hash("studyid")
        usubjid_hash = schema.get_column_hash("usubjid")
        domain_hash = schema.get_column_hash("domain")
        seq_col = f"{table_name}seq"
        seq_hash = schema.get_column_hash(seq_col)

        if studyid_hash:
            index_queries.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_studyid ON {table_hash}({studyid_hash})")
        if usubjid_hash:
            index_queries.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_usubjid ON {table_hash}({usubjid_hash})")
        if studyid_hash and usubjid_hash:
            index_queries.append(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_studyid_usubjid ON {table_hash}({studyid_hash}, {usubjid_hash})"  # noqa
            )
        if domain_hash:
            index_queries.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_domain ON {table_hash}({domain_hash})")
        if seq_hash:
            index_queries.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_seq ON {table_hash}({seq_hash})")

        for idx_query in index_queries:
            pgi.execute_sql(idx_query)

        pgi.execute_sql(f"ANALYZE {table_hash}")
