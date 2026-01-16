from pathlib import Path

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.codelist_reader import CodelistReader
from cdisc_rules_engine.services import logger


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


def populate_codelists(pgi: PostgresQLInterface, path: Path = None):
    """
    Create tables to store CDISC codelists.
    If path is not provided, defaults to the cache.
    """
    if not path:
        path = Path(__file__).parents[3] / Path(DefaultFilePaths.CACHE.value)
        logger.info(f"No codelists path provided, defaulting to: {path}")

    if not path.exists():
        logger.warning(f"Codelists path {path} does not exist. Skipping population.")
        return

    schema = _schema()
    pgi.create_table(schema)

    for file_path in path.iterdir():
        try:
            reader = CodelistReader(str(file_path))
            codelist_data = reader.read()

            if codelist_data:
                pgi.insert_data(schema.hash, codelist_data)
                logger.info(f"Loaded codelist from {file_path.name}")

        except Exception as e:
            logger.debug(f"Skipping file {file_path.name}: {e}")
            continue
