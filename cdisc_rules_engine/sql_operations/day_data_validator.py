from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

ISO_PATTERN = r"^\d{4}(-\d{2}(-\d{2}([T ]\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}:?\d{2})?)?)?)?$"


class SqlDayDataValidatorOperation(SqlBaseOperation):

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

        target_date_col = self.data_service.pgi.schema.get_column(joined_table.name, self.params.target)

        if not joined_table.has_column("RFSTDTC"):
            raise ValueError("RFSTDTC column not found in joined table")

        rfstdtc_date_col = self.data_service.pgi.schema.get_column(joined_table.name, "RFSTDTC")

        id_col = self.data_service.pgi.schema.get_column_hash(joined_table.name, "id")

        query = self._study_day_sql(
            target_date_col.hash, rfstdtc_date_col.hash, joined_table.hash, where=f"{id_col} = $id"
        )

        return SqlOperationResult(query=query, type="constant", subtype="Num", params={"$id": "id"})

    @staticmethod
    def _study_day_sql(target_col: str, reference_col: str, table: str, where: str) -> str:
        cleaned = f"""SELECT
                CASE WHEN {target_col}::text ~ '{ISO_PATTERN}' THEN {target_col}::text END AS target,
                CASE WHEN {reference_col}::text ~ '{ISO_PATTERN}' THEN {reference_col}::text END AS reference
            FROM {table}
            WHERE {where}
            OFFSET 0"""
        precision = f"""SELECT c.target, c.reference, LEAST(LENGTH(c.target), LENGTH(c.reference)) AS min_len
            FROM ({cleaned}) c
            OFFSET 0"""
        slice_length = "CASE WHEN p.min_len = 4 THEN 4 WHEN p.min_len = 7 THEN 7 ELSE 10 END"
        format_mask = "CASE WHEN p.min_len = 4 THEN 'YYYY' WHEN p.min_len = 7 THEN 'YYYY-MM' ELSE 'YYYY-MM-DD' END"
        dated = f"""SELECT
                TO_DATE(LEFT(p.target, {slice_length}), {format_mask}) AS target,
                TO_DATE(LEFT(p.reference, {slice_length}), {format_mask}) AS reference
            FROM ({precision}) p
            OFFSET 0"""
        return f"""SELECT
            CASE
                WHEN d.target IS NULL OR d.reference IS NULL THEN NULL
                WHEN d.target >= d.reference THEN (d.target - d.reference) + 1
                ELSE d.target - d.reference
            END AS value
        FROM ({dated}) d"""
