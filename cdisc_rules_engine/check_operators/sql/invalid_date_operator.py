from .base_sql_operator import BaseSqlOperator


# Regex patterns for date format validation
YEAR_FORMAT_PATTERN = "^-?[0-9]{4}$"
PARTIAL_DATE_FORMAT_PATTERN = "^-?[0-9]{4}-[0-9]{2}$"
UNCERTAINTY_YEAR_PATTERN = "^-?[0-9]{4}--$"
UNCERTAINTY_MONTH_PATTERN = "^-?[0-9]{4}-[0-9]{2}--$"
ISO_DATE_FORMAT_PATTERN = (
    "^-?[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?(\\.[0-9]+)?([+-][0-9]{2}:?[0-9]{2}|Z)?)?$"
)
INVALID_HOUR_PATTERN = "T(2[4-9]|[3-9][0-9]):"
INVALID_MINUTE_SECOND_PATTERN = ":([6-9][0-9])([^0-9]|$)"


class InvalidDateOperator(BaseSqlOperator):
    """
    Operator for checking if date is invalid.

    This implementation matches the business_rules.utils.is_valid_date logic:
    - Simple years (YYYY) are valid if reasonable (1-9999)
    - Partial dates (YYYY-MM) are valid with basic validation
    - Full ISO dates are valid if they can be parsed as timestamps
    - Malformed formats are invalid
    """

    def _is_year_format_valid(self, target):
        """Check if the date is a valid 4-digit year format (YYYY)."""
        return f"{self._column_sql(target)} ~ '{YEAR_FORMAT_PATTERN}'"

    def _is_partial_date_format_valid(self, target):
        """Check if the date is a valid partial date format (YYYY-MM)."""
        return f"""
            {self._column_sql(target)} ~ '{PARTIAL_DATE_FORMAT_PATTERN}' AND
            CAST(RIGHT({self._column_sql(target)}, 2) AS INTEGER) BETWEEN 1 AND 12
        """

    def _is_uncertainty_pattern_valid(self, target):
        """Check if the date is a valid uncertainty pattern with double dashes."""
        return f"""
            {self._column_sql(target)} ~ '{UNCERTAINTY_YEAR_PATTERN}' OR
            {self._column_sql(target)} ~ '{UNCERTAINTY_MONTH_PATTERN}'
        """

    def _is_iso_format_pattern(self, target):
        """Check if the date matches the complete ISO date format pattern."""
        return f"""
            {self._column_sql(target)} ~ '{ISO_DATE_FORMAT_PATTERN}'
        """

    def _has_invalid_time_components(self, target):
        """Check for invalid time components in ISO datetime."""
        return f"""
            {self._column_sql(target)} ~ '{INVALID_HOUR_PATTERN}' OR
            {self._column_sql(target)} ~ '{INVALID_MINUTE_SECOND_PATTERN}'
        """

    def _has_invalid_basic_date_components(self, target):
        """Check for invalid basic date components (month/day out of range)."""
        return f"""
            CAST(SUBSTRING({self._column_sql(target)}, 6, 2) AS INTEGER) > 12 OR
            CAST(SUBSTRING({self._column_sql(target)}, 6, 2) AS INTEGER) < 1 OR
            CAST(SUBSTRING({self._column_sql(target)}, 9, 2) AS INTEGER) > 31 OR
            CAST(SUBSTRING({self._column_sql(target)}, 9, 2) AS INTEGER) < 1
        """

    def _has_calendar_date_errors(self, target):
        """Check for calendar-specific date errors (leap years, days per month)."""
        return f"""
            SELECT CASE
                -- Feb 29 in non-leap years (proper leap year calculation)
                WHEN SUBSTRING({self._column_sql(target)}, 6, 5) = '02-29'
                 AND NOT (
                     (CAST(SUBSTRING({self._column_sql(target)}, 1, 4) AS INTEGER) % 4 = 0
                      AND CAST(SUBSTRING({self._column_sql(target)}, 1, 4) AS INTEGER) % 100 != 0)
                     OR CAST(SUBSTRING({self._column_sql(target)}, 1, 4) AS INTEGER) % 400 = 0
                 ) THEN TRUE
                -- Apr 31, Jun 31, Sep 31, Nov 31 (months with 30 days)
                WHEN SUBSTRING({self._column_sql(target)}, 6, 5) IN
                     ('04-31', '06-31', '09-31', '11-31') THEN TRUE
                -- Feb 30, Feb 31 (February never has 30+ days)
                WHEN SUBSTRING({self._column_sql(target)}, 6, 5) IN
                     ('02-30', '02-31') THEN TRUE
                -- Day 00 for any month
                WHEN SUBSTRING({self._column_sql(target)}, 9, 2) = '00' THEN TRUE
                ELSE FALSE
            END
        """

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        op_name = f"{target}_is_invalid_date"

        def sql():
            return f"""
            CASE
                WHEN {self._is_empty_sql(target)} THEN FALSE
                ELSE (
                    CASE
                        -- Handle 4-digit year format only (YYYY)
                        WHEN {self._is_year_format_valid(target)} THEN FALSE
                        -- Handle partial date format (YYYY-MM) with validation
                        WHEN {self._is_partial_date_format_valid(target)} THEN FALSE
                        -- Handle uncertainty patterns with double dashes
                        WHEN {self._is_uncertainty_pattern_valid(target)} THEN FALSE
                        -- Handle complete ISO date format with validation
                        WHEN {self._is_iso_format_pattern(target)} THEN
                            CASE
                                WHEN {self._has_invalid_time_components(target)} THEN TRUE
                                WHEN {self._has_invalid_basic_date_components(target)} THEN TRUE
                                ELSE ({self._has_calendar_date_errors(target)})
                            END
                        -- Any other format is invalid
                        ELSE TRUE
                    END
                )
            END
            """

        return self._do_check_operator(op_name, sql)
