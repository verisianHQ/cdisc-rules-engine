from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDayDataValidatorOperation(SqlBaseOperation):

    def _build_to_date_comparison_sql(self, wrapped_target: str, wrapped_comparator: str) -> str:
        """
        Generates SQL to validate ISO formats, slice both dates to the lower precision,
        and cast both to DATE using TO_DATE with a dynamic format mask based on length.
        """
        # Strict zero-padded ISO-8601 validation pattern
        iso_pattern = r"^\d{4}(-\d{2}(-\d{2}([T ]\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}:?\d{2})?)?)?)?$"

        clean_target = f"CASE WHEN {wrapped_target}::text ~ '{iso_pattern}' THEN {wrapped_target}::text ELSE NULL END"
        clean_comp = (
            f"CASE WHEN {wrapped_comparator}::text ~ '{iso_pattern}' THEN {wrapped_comparator}::text ELSE NULL END"
        )

        min_len = f"LEAST(LENGTH({clean_target}), LENGTH({clean_comp}))"

        format_mask = f"""CASE
            WHEN {min_len} = 4 THEN 'YYYY'
            WHEN {min_len} = 7 THEN 'YYYY-MM'
            ELSE 'YYYY-MM-DD'
        END"""

        slice_length = f"""CASE
            WHEN {min_len} = 4 THEN 4
            WHEN {min_len} = 7 THEN 7
            ELSE 10
        END"""

        target_date_sql = f"TO_DATE(LEFT({clean_target}, {slice_length}), {format_mask})"
        comp_date_sql = f"TO_DATE(LEFT({clean_comp}, {slice_length}), {format_mask})"

        return f"{target_date_sql}", f"{comp_date_sql}"

    def _execute_operation(self):
        """
        Calculate Study Day (--DY) values by computing the difference between
        a date-time column (--DTC) and the reference start date (RFSTDTC) from the DM dataset.

        CDISC Algorithm:
        - If --DTC >= RFSTDTC: --DY = (--DTC date) - (RFSTDTC date) + 1
        - If --DTC < RFSTDTC: --DY = (--DTC date) - (RFSTDTC date)
        - No Study Day 0 exists (goes from -1 to +1)
        """
        current_table = self.data_service.pgi.schema.get_table(self.params.domain)
        if not current_table:
            raise ValueError(f"Table for domain {self.params.domain} not found")

        if current_table.has_column("RFSTDTC"):
            joined_table = current_table
        else:
            dm_table = self.data_service.pgi.schema.get_table("DM")
            if not dm_table:
                return SqlOperationResult(query="SELECT 0 AS value", type="constant", subtype="Num")
            joined_table = SqlJoinMerge.perform_join(
                pgi=self.data_service.pgi,
                left=current_table,
                right=dm_table,
                pivot_left=["USUBJID"],
                pivot_right=["USUBJID"],
                type="LEFT",
            )

        if not joined_table.has_column(self.params.target):
            return SqlOperationResult(query="SELECT 0 AS value", type="constant", subtype="Num")

        # target_date_col = self.data_service.pgi.generate_date_column(joined_table.name, self.params.target)
        target_date_col = self.data_service.pgi.schema.get_column(joined_table.name, self.params.target)

        if not joined_table.has_column("RFSTDTC"):
            raise ValueError("RFSTDTC column not found in joined table")

        # rfstdtc_date_col = self.data_service.pgi.generate_date_column(joined_table.name, "RFSTDTC")
        rfstdtc_date_col = self.data_service.pgi.schema.get_column(joined_table.name, "RFSTDTC")

        target_date_col, rfstdtc_date_col = self._build_to_date_comparison_sql(
            target_date_col.hash, rfstdtc_date_col.hash
        )

        id_col = self.data_service.pgi.schema.get_column_hash(joined_table.name, "id")

        query = f"""
        SELECT
            CASE
                WHEN {target_date_col} IS NULL OR {rfstdtc_date_col} IS NULL THEN NULL
                WHEN {target_date_col} >= {rfstdtc_date_col} THEN
                    ({target_date_col} - {rfstdtc_date_col}) + 1
                ELSE
                    ({target_date_col} - {rfstdtc_date_col})
            END AS value
        FROM {joined_table.hash}
        WHERE {id_col} = $id
        """

        return SqlOperationResult(query=query, type="constant", subtype="Num", params={"$id": "id"})
