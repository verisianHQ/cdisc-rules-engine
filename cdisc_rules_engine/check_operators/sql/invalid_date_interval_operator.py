from .base_sql_operator import BaseSqlOperator
from .invalid_date_operator import InvalidDateOperator
from .invalid_duration_operator import InvalidDurationOperator

# Exactly one solidus, with something on each side of it
INTERVAL_SHAPE_PATTERN = "^[^/]+/[^/]+$"


class InvalidDateIntervalOperator(BaseSqlOperator):
    """
    Operator for checking if an ISO 8601 date interval is invalid.

    Valid forms, with the two parts separated by a solidus:
    - <start datetime>/<end datetime>   e.g. 2023-01-01T08:00:00/2023-01-02T08:00:00
    - <start datetime>/<duration>       e.g. 2023-01-01T08:00:00/P1DT2H
    - <duration>/<end datetime>         e.g. P1DT2H/2023-01-02T08:00:00

    Each datetime part is validated with the same rules as invalid_date, and each duration
    part with the same rules as invalid_duration (negative durations disallowed - an
    interval's direction comes from the order of its parts). Empty values are invalid.
    """

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        value = f"CAST({self._column_sql(target)} AS TEXT)"

        start_invalid_date = InvalidDateOperator.invalid_date_sql("parts.interval_start")
        end_invalid_date = InvalidDateOperator.invalid_date_sql("parts.interval_end")
        start_invalid_duration = InvalidDurationOperator.invalid_duration_sql("parts.interval_start")
        end_invalid_duration = InvalidDurationOperator.invalid_duration_sql("parts.interval_end")

        def sql():
            return f"""
            CASE
                WHEN {self._is_empty_sql(target)} THEN TRUE
                WHEN {value} !~ '{INTERVAL_SHAPE_PATTERN}' THEN TRUE
                ELSE (
                    SELECT CASE
                        WHEN NOT checks.start_invalid_date AND NOT checks.end_invalid_date THEN FALSE
                        WHEN NOT checks.start_invalid_date AND NOT checks.end_invalid_duration THEN FALSE
                        WHEN NOT checks.start_invalid_duration AND NOT checks.end_invalid_date THEN FALSE
                        ELSE TRUE
                    END
                    FROM (
                        SELECT
                            ({start_invalid_date}) AS start_invalid_date,
                            ({end_invalid_date}) AS end_invalid_date,
                            ({start_invalid_duration}) AS start_invalid_duration,
                            ({end_invalid_duration}) AS end_invalid_duration
                        FROM (
                            SELECT
                                SPLIT_PART({value}, '/', 1) AS interval_start,
                                SPLIT_PART({value}, '/', 2) AS interval_end
                        ) AS parts
                    ) AS checks
                )
            END
            """

        return self._do_check_operator(sql)
