
    from cdisc_rules_engine.readers.data_reader import DataReader


def _create_sql_tables_from_dataset_paths(self) -> None:
        """
        Iterate through dataset files in `self.datasets_path`
        and create corresponding SQL tables.
        """
        if not self.datasets_path or not self.datasets_path.exists():
            logger.info("No datasets path provided or path doesn't exist")
            return

        self.pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))

        timestamp = datetime.now().astimezone()

        for file_path in self.datasets_path.iterdir():
            self._process_dataset_file(file_path, timestamp)

    def _process_dataset_file(self, file_path: Path, timestamp: datetime) -> None:
        """Process a single dataset file."""
        try:
            reader = DataReader(str(file_path))
            metadata_info = reader.read_metadata()

            # force table_name to be lowercase
            table_name = file_path.stem.lower()

            logger.info(f"Loading dataset {file_path.name} into table {table_name}")

            self._create_table_with_indexes(table_name, metadata_info)

            metadata_rows = []
            first_chunk_processed = False

            for chunk_data in reader.read():
                # force lowercase on columns
                chunk_data = [{k.lower(): v for k, v in row} for row in chunk_data.items()]
                if not first_chunk_processed and chunk_data:
                    first_chunk = chunk_data[0]

                    metadata_rows = self._build_metadata_rows(
                        file_path, table_name, metadata_info, first_chunk, timestamp
                    )
                    first_chunk_processed = True

                if chunk_data:
                    self.pgi.insert_data(table_name, chunk_data)

            if metadata_rows:
                self.pgi.insert_data("data_metadata", metadata_rows)

            logger.info(f"Successfully loaded {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")

    def _create_table_with_indexes(self, table_name: str, metadata: dict) -> None:
        """Create table and add indexes for CDISC variables."""
        self.pgi.create_table_from_metadata(table_name, metadata)

        for col in ("usubjid", "studyid", "domain", "seq", "idvar", "idvarval"):
            if col in [var["name"].lower() for var in metadata["variables"]]:
                self.pgi.execute_sql(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col.lower()} ON {table_name}({col})"
                )