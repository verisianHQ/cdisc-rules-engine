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

        Both dates are read from the row being checked, so a rule using this operation must merge DM.
        """
        table = self.data_service.pgi.schema.get_table(self.params.table)
        if not table:
            raise ValueError(f"Table {self.params.table} not found")
        # target before RFSTDTC: a dataset without the target isn't checked, even when DM has no RFSTDTC
        if not table.has_column(self.params.target):
            return SqlOperationResult(query="SELECT 0 AS value", type="constant", subtype="Num")
        if not table.has_column("RFSTDTC"):
            raise ValueError("RFSTDTC not found: rules using the dy operation must merge DM")

        return SqlOperationResult(
            query=self._study_day_sql("$dtc", "$rfstdtc"),
            type="constant",
            subtype="Num",
            params={"$dtc": self.params.target, "$rfstdtc": "RFSTDTC"},
        )

    @staticmethod
    def _study_day_sql(target: str, reference: str) -> str:
        cleaned = f"""SELECT
                CASE WHEN {target}::text ~ '{ISO_PATTERN}' THEN {target}::text END AS target,
                CASE WHEN {reference}::text ~ '{ISO_PATTERN}' THEN {reference}::text END AS reference
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
