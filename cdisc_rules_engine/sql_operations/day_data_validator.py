from cdisc_rules_engine.data_service.merges.join import missing_match_key_error
from cdisc_rules_engine.data_service.util import numeric_aware_equals_sql
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

        Both dates are read from the row being checked ($1, $2), never looked up by row id in another table,
        so rows dropped or duplicated by the rule's merges can't pair a row with another row's dates.
        """
        schema = self.data_service.pgi.schema
        table = (schema.get_table(self.params.table) if self.params.table else None) or schema.get_table(
            self.params.domain
        )
        if not table:
            raise ValueError(f"Table for domain {self.params.domain} not found")

        if table.has_column("RFSTDTC"):
            # the rule merged DM, so the row carries its own RFSTDTC
            if not table.has_column(self.params.target):
                return SqlOperationResult(query="SELECT 0 AS value", type="constant", subtype="Num")
            reference_sql, reference_column = "$2", "RFSTDTC"
        else:
            dm_table = schema.get_table("DM")
            if not dm_table:
                return SqlOperationResult(query="SELECT 0 AS value", type="constant", subtype="Num")
            usubjid_match = self._usubjid_match_sql(table, dm_table)
            if not table.has_column(self.params.target):
                return SqlOperationResult(query="SELECT 0 AS value", type="constant", subtype="Num")
            rfstdtc = dm_table.get_column("RFSTDTC")
            if rfstdtc is None:
                raise ValueError("RFSTDTC column not found in joined table")
            # the subject's DM record, the first one if DM repeats the subject
            reference_sql = (
                f"(SELECT dm.{rfstdtc.hash} FROM {dm_table.hash} dm WHERE {usubjid_match} ORDER BY dm.id LIMIT 1)"
            )
            reference_column = "USUBJID"

        query = self._study_day_sql("$1", reference_sql)
        return SqlOperationResult(
            query=query, type="constant", subtype="Num", params={"$1": self.params.target, "$2": reference_column}
        )

    def _usubjid_match_sql(self, table, dm_table) -> str:
        """DM's USUBJID equal to the row's USUBJID ($2), compared the same way SqlJoinMerge joins on it."""
        row_usubjid, dm_usubjid = table.get_column("usubjid"), dm_table.get_column("usubjid")
        if row_usubjid is None:
            raise missing_match_key_error(self.data_service.pgi, "usubjid", table)
        if dm_usubjid is None:
            raise missing_match_key_error(self.data_service.pgi, "usubjid", dm_table)
        if row_usubjid.type == dm_usubjid.type:
            return f"dm.{dm_usubjid.hash} = $2"
        return numeric_aware_equals_sql(f"dm.{dm_usubjid.hash}", "$2")

    @staticmethod
    def _study_day_sql(target_sql: str, reference_sql: str) -> str:
        dates = f"""SELECT {target_sql}::text AS target, {reference_sql}::text AS reference OFFSET 0"""
        cleaned = f"""SELECT
                CASE WHEN v.target ~ '{ISO_PATTERN}' THEN v.target END AS target,
                CASE WHEN v.reference ~ '{ISO_PATTERN}' THEN v.reference END AS reference
            FROM ({dates}) v
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
