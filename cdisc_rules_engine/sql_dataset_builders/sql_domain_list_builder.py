from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlDomainListDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Domain Presence Check rules.
    Creates a physical table with one row containing all available domains as columns.
    Mirrors DomainListDatasetBuilder which builds a wide DataFrame on-the-fly.

    Example table structure:
       AE      EC      DM
    -------|--------|--------
    ae.xpt | ec.xpt | dm.xpt
    """

    def build(self) -> str:
        """
        Create (or replace) a domains catalog table and return the table name.
        The table has one row with each domain as a column containing its filename.
        """
        table_name = "domains_catalog_table"
        self.data_service.pgi.execute_sql(f"DROP TABLE IF EXISTS {table_name};")

        # 2. Get a list of all available domains from the data_metadata table
        domains_query = """
        SELECT DISTINCT dataset_domain, dataset_filename
        FROM data_metadata
        WHERE dataset_domain IS NOT NULL
        ORDER BY dataset_domain;
        """
        self.data_service.pgi.execute_sql(domains_query)
        domains = self.data_service.pgi.fetch_all()

        if not domains:
            # No domains found, create an empty table
            create_table_sql = f"CREATE TABLE {table_name} (_placeholder INT);"
        else:
            # 3. Build the column list for the wide table's SELECT statement
            column_selects = [f"'{row['dataset_filename']}'::TEXT AS {row['dataset_domain']}" for row in domains]

            # 4. Use CREATE TABLE AS SELECT to create and populate the table in one step
            create_table_sql = f"""
            CREATE TABLE {table_name} AS
            SELECT {', '.join(column_selects)};
            """

        self.data_service.pgi.execute_sql(create_table_sql)
        return table_name
