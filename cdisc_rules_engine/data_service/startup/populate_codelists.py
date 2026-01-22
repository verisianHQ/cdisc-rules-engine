from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.codelist_reader import CodelistReader
from cdisc_rules_engine.services import logger

ROOT_PATH = Path(__file__).parents[3]


def _schema():
    table = SqlTableSchema.static(StaticTables.IG_CODELIST_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version_date", hash="version_date", type="Char"))
    table.add_column(SqlColumnSchema(name="item_code", hash="item_code", type="Char"))
    table.add_column(SqlColumnSchema(name="codelist_code", hash="codelist_code", type="Char"))
    table.add_column(SqlColumnSchema(name="extensible", hash="extensible", type="Char"))
    table.add_column(SqlColumnSchema(name="name", hash="name", type="Char"))
    table.add_column(SqlColumnSchema(name="value", hash="value", type="Char"))
    table.add_column(SqlColumnSchema(name="synonym", hash="synonym", type="Char"))
    table.add_column(SqlColumnSchema(name="definition", hash="definition", type="Char"))
    table.add_column(SqlColumnSchema(name="term", hash="term", type="Char"))
    table.add_column(SqlColumnSchema(name="standard_and_date", hash="standard_and_date", type="Char"))
    return table


def _get_codelists_from_default_cache() -> List[Path]:
    cache_path = ROOT_PATH / Path(DefaultFilePaths.CACHE.value)

    if not cache_path.exists():
        logger.warning(f"Cache path does not exist: {cache_path}")
        return []

    codelists = []
    for file_path in cache_path.glob("*"):
        if file_path.is_file() and CodelistReader.FILENAME_PATTERN.match(file_path.name):
            codelists.append(file_path)

    return codelists


def _validate_user_paths(codelists: Optional[List[str]]) -> Tuple[List[Path], List[Path]]:
    valid_user_paths = []
    invalid_user_paths = []

    if codelists:
        for path_str in codelists:
            path = ROOT_PATH / Path(path_str)
            if path.exists() and path.is_file():
                valid_user_paths.append(path)
            else:
                invalid_user_paths.append(path)
    return valid_user_paths, invalid_user_paths


def _determine_files_to_load(codelists: Optional[List[str]], cache_path: str) -> Optional[List[Path]]:
    valid_user_paths, invalid_user_paths = _validate_user_paths(codelists)

    if codelists is not None:
        if not invalid_user_paths and valid_user_paths:
            return valid_user_paths

        if invalid_user_paths:
            if not cache_path:
                raise ValueError(f"Provided codelist paths do not exist or are invalid: {invalid_user_paths}")
            logger.warning(f"Provided codelist paths are invalid: {invalid_user_paths}. Falling back to cache.")
            return _get_codelists_from_default_cache()

        if not cache_path:
            logger.info("Empty codelists list provided and cache disabled.")
            return None
        return _get_codelists_from_default_cache()

    if not cache_path:
        logger.info("No codelists provided and cache disabled.")
        return None
    return _get_codelists_from_default_cache()


def populate_codelists(
    pgi: PostgresQLInterface,
    cache_path: str,
    codelists: List[Union[str, Dict]],
):
    """Populate the codelists table in the database."""
    if codelists:
        # TODO: Handle define extensible dict records
        codelists = [item for item in codelists if isinstance(item, str)]

    files_to_load = _determine_files_to_load(codelists, cache_path)

    if not files_to_load:
        if files_to_load is not None:
            logger.warning("No codelist files found to load.")
        return

    schema = _schema()
    pgi.create_table(schema)

    for file_path in files_to_load:
        try:
            reader = CodelistReader(str(file_path))
            codelist_data = reader.read()

            if codelist_data:
                pgi.insert_data(schema.hash, codelist_data)
                logger.info(f"Loaded codelist from {file_path.name}")
            else:
                logger.warning(f"No data found in codelist file: {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to load codelist {file_path.name}: {e}")
            continue
