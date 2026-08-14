from pathlib import Path
from typing import Optional

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.metadata_standards_reader import MetadataStandardsReader
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.ingestion_progress import (
    DisabledIngestionProgressReporter,
    IngestionProgressReporter,
)


def _dataset_schema():
    table = SqlTableSchema.static(StaticTables.IG_DATASETS_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version", hash="version", type="Char"))
    table.add_column(SqlColumnSchema(name="class", hash="class", type="Char"))
    table.add_column(SqlColumnSchema(name="dataset_name", hash="dataset_name", type="Char"))
    table.add_column(SqlColumnSchema(name="dataset_label", hash="dataset_label", type="Char"))
    table.add_column(SqlColumnSchema(name="structure", hash="structure", type="Char"))
    table.add_column(SqlColumnSchema(name="structure_name", hash="structure_name", type="Char"))
    table.add_column(SqlColumnSchema(name="structure_description", hash="structure_description", type="Char"))
    table.add_column(SqlColumnSchema(name="subclass", hash="subclass", type="Char"))
    table.add_column(SqlColumnSchema(name="notes", hash="notes", type="Char"))
    return table


def _variable_schema():
    table = SqlTableSchema.static(StaticTables.IG_VARIABLES_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version", hash="version", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_order", hash="variable_order", type="Num"))
    table.add_column(SqlColumnSchema(name="class", hash="class", type="Char"))
    table.add_column(SqlColumnSchema(name="dataset_name", hash="dataset_name", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_name", hash="variable_name", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_label", hash="dataset_label", type="Char"))
    table.add_column(SqlColumnSchema(name="structure_name", hash="structure_name", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_set", hash="variable_set", type="Char"))
    table.add_column(SqlColumnSchema(name="type", hash="type", type="Char"))
    table.add_column(SqlColumnSchema(name="codelist_code", hash="codelist_code", type="Char"))
    table.add_column(SqlColumnSchema(name="submission_value", hash="submission_value", type="Char"))
    table.add_column(SqlColumnSchema(name="value_domain", hash="value_domain", type="Char"))
    table.add_column(SqlColumnSchema(name="value_list", hash="value_list", type="Char"))
    table.add_column(SqlColumnSchema(name="role", hash="role", type="Char"))
    table.add_column(SqlColumnSchema(name="notes", hash="notes", type="Char"))
    table.add_column(SqlColumnSchema(name="core", hash="core", type="Char"))
    return table


def populate_standards(
    pgi: PostgresQLInterface,
    path: Path = None,
    progress_reporter: Optional[IngestionProgressReporter] = None,
):
    """
    Create all necessary SQL tables for IG standards.
    """
    if not path:
        logger.info("No metadata standards path provided, will use cached IG metadata")
        # TODO: Use a default path or configuration for metadata
        return

    if not path.exists():
        logger.warning(f"Metadata standards path {path} does not exist")
        return

    reporter = progress_reporter or DisabledIngestionProgressReporter()

    ds_schema = _dataset_schema()
    var_schema = _variable_schema()
    pgi.create_table(ds_schema)
    # TODO: INDEX

    files = [file_path for file_path in path.iterdir() if file_path.is_file()]
    total_bytes = sum(file_path.stat().st_size for file_path in files)
    reporter.start(total_files=len(files), total_bytes=total_bytes)

    try:
        for file_index, file_path in enumerate(files):
            try:
                reader = MetadataStandardsReader(str(file_path))
                ig_data = reader.read()
                datasets = ig_data.get("datasets") or []
                variables = ig_data.get("variables") or []
                total_rows = len(datasets) + len(variables)

                reporter.start_file(
                    file_index=file_index,
                    total_files=len(files),
                    file_name=file_path.name,
                    file_bytes=file_path.stat().st_size,
                    total_rows=total_rows,
                )

                if datasets:
                    pgi.insert_data(ds_schema.hash, datasets)
                    reporter.report_chunk(len(datasets))

                if variables:
                    pgi.insert_data(var_schema.hash, variables)
                    reporter.report_chunk(len(variables))

                reporter.end_file(total_rows)
                logger.info(f"Loaded IG metadata from {file_path.name}")

            except Exception as e:
                logger.error(f"Failed to load IG metadata {file_path.name}: {e}")
                continue
    finally:
        reporter.finish()
