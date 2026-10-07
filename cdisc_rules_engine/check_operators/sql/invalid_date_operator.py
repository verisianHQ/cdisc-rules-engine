from .base_sql_operator import BaseSqlOperator


# Regex patterns for date format validation
YEAR_FORMAT_PATTERN = "^-?[0-9]{4}$"
PARTIAL_DATE_FORMAT_PATTERN = "^-?[0-9]{4}-[0-9]{2}$"
UNCERTAINTY_YEAR_PATTERN = "^-?[0-9]{4}--$"
UNCERTAINTY_MONTH_PATTERN = "^-?[0-9]{4}-[0-9]{2}--$"
# Patterns for incomplete/invalid formats
INVALID_YEAR_DASH_PATTERN = "^-?[0-9]{4}-$"  # YYYY- (invalid)
INVALID_MONTH_DASH_PATTERN = "^-?[0-9]{4}-[0-9]{2}-$"  # YYYY-MM- (invalid)
ISO_DATE_FORMAT_PATTERN = (
    "^-?[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?(\\.[0-9]+)?([+-][0-9]{2}:?[0-9]{2}|Z)?)?$"
)
INVALID_HOUR_PATTERN = "T(2[4-9]|[3-9][0-9]):"
INVALID_MINUTE_SECOND_PATTERN = ":([6-9][0-9])([^0-9]|$)"

# Date range patterns for uncertainty handling
DATE_RANGE_PATTERN = "/"
TIME_UNCERTAINTY_PATTERN = "-:"


class InvalidDateOperator(BaseSqlOperator):
    """
    Operator for checking if date is invalid.

    This implementation matches the business_rules.utils.is_valid_date logic:
    - Simple years (YYYY) are valid if reasonable (1-9999)
    - Partial dates (YYYY-MM) are valid with basic validation
    - Full ISO dates are valid if they can be parsed as timestamps
    - Malformed formats are invalid
    """

    @staticmethod
    def _is_year_format_valid(value):
        """Check if the date is a valid 4-digit year format (YYYY)."""
        return f"{value} ~ '{YEAR_FORMAT_PATTERN}'"

    @staticmethod
    def _is_partial_date_format_valid(value):
        """Check if the date is a valid partial date format (YYYY-MM)."""
        return f"""
            {value} ~ '{PARTIAL_DATE_FORMAT_PATTERN}' AND
            CAST(RIGHT({value}, 2) AS INTEGER) BETWEEN 1 AND 12
        """

    @staticmethod
    def _is_uncertainty_pattern_valid(value):
        """Check if the date is a valid uncertainty pattern with double dashes."""
        return f"""
            {value} ~ '{UNCERTAINTY_YEAR_PATTERN}' OR
            {value} ~ '{UNCERTAINTY_MONTH_PATTERN}'
        """

    @staticmethod
    def _has_invalid_incomplete_patterns(value):
        """Check if the date has invalid incomplete format like '2023-' or '2023-05-'."""
        return f"""
            {value} ~ '{INVALID_YEAR_DASH_PATTERN}' OR
            {value} ~ '{INVALID_MONTH_DASH_PATTERN}'
        """

    @staticmethod
    def _is_date_range_pattern(value):
        """Check if the date contains a range separator (/)."""
        return f"{value} ~ '{DATE_RANGE_PATTERN}'"

    @staticmethod
    def _has_time_uncertainty_pattern(value):
        """Check if the date contains time uncertainty pattern (-:)."""
        return f"{value} ~ '{TIME_UNCERTAINTY_PATTERN}'"

    @staticmethod
    def _is_valid_date_range(value):
        """Check if a date range pattern is valid (basic validation)."""
        return f"""
            CASE
                -- Simple month ranges: YYYY-MM/YYYY-MM
                WHEN {value} ~ '^[0-9]{{4}}-[0-9]{{2}}/[0-9]{{4}}-[0-9]{{2}}$' THEN TRUE
                -- Full date ranges: YYYY-MM-DD/YYYY-MM-DD (with possible time components)
                WHEN {value} ~
                     '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}/[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN TRUE
                ELSE FALSE
            END
        """

    @staticmethod
    def _is_iso_format_pattern(value):
        """Check if the date matches the complete ISO date format pattern."""
        return f"""
            {value} ~ '{ISO_DATE_FORMAT_PATTERN}'
        """

    @staticmethod
    def _has_invalid_time_components(value):
        """Check for invalid time components in ISO datetime."""
        return f"""
            {value} ~ '{INVALID_HOUR_PATTERN}' OR
            {value} ~ '{INVALID_MINUTE_SECOND_PATTERN}'
        """

    @staticmethod
    def _has_invalid_basic_date_components(value):
        """Check for invalid basic date components (month/day out of range)."""
        return f"""
            CAST(SUBSTRING({value}, 6, 2) AS INTEGER) > 12 OR
            CAST(SUBSTRING({value}, 6, 2) AS INTEGER) < 1 OR
            CAST(SUBSTRING({value}, 9, 2) AS INTEGER) > 31 OR
            CAST(SUBSTRING({value}, 9, 2) AS INTEGER) < 1
        """

    @staticmethod
    def _has_calendar_date_errors(value):
        """Check for calendar-specific date errors (leap years, days per month)."""
        return f"""
            SELECT CASE
                -- Feb 29 in non-leap years (proper leap year calculation)
                WHEN SUBSTRING({value}, 6, 5) = '02-29'
                 AND NOT (
                     (CAST(SUBSTRING({value}, 1, 4) AS INTEGER) % 4 = 0
                      AND CAST(SUBSTRING({value}, 1, 4) AS INTEGER) % 100 != 0)
                     OR CAST(SUBSTRING({value}, 1, 4) AS INTEGER) % 400 = 0
                 ) THEN TRUE
                -- Apr 31, Jun 31, Sep 31, Nov 31 (months with 30 days)
                WHEN SUBSTRING({value}, 6, 5) IN
                     ('04-31', '06-31', '09-31', '11-31') THEN TRUE
                -- Feb 30, Feb 31 (February never has 30+ days)
                WHEN SUBSTRING({value}, 6, 5) IN
                     ('02-30', '02-31') THEN TRUE
                -- Day 00 for any month
                WHEN SUBSTRING({value}, 9, 2) = '00' THEN TRUE
                ELSE FALSE
            END
        """

    @staticmethod
    def invalid_date_sql(value):
        """TRUE if the non-empty SQL text expression `value` is not a valid date."""
        return f"""
            CASE
                -- Handle invalid incomplete patterns (YYYY- and YYYY-MM-) first - these are invalid
                WHEN {InvalidDateOperator._has_invalid_incomplete_patterns(value)} THEN TRUE
                -- Handle time uncertainty patterns with -: (these are invalid)
                WHEN {InvalidDateOperator._has_time_uncertainty_pattern(value)} THEN TRUE
                -- Handle date ranges with / - check if they're valid ranges
                WHEN {InvalidDateOperator._is_date_range_pattern(value)} THEN
                    NOT ({InvalidDateOperator._is_valid_date_range(value)})
                -- Handle 4-digit year format only (YYYY)
                WHEN {InvalidDateOperator._is_year_format_valid(value)} THEN FALSE
                -- Handle partial date format (YYYY-MM) with validation
                WHEN {InvalidDateOperator._is_partial_date_format_valid(value)} THEN FALSE
                -- Handle uncertainty patterns with double dashes
                WHEN {InvalidDateOperator._is_uncertainty_pattern_valid(value)} THEN FALSE
                -- Handle complete ISO date format with validation
                WHEN {InvalidDateOperator._is_iso_format_pattern(value)} THEN
                    CASE
                        WHEN {InvalidDateOperator._has_invalid_time_components(value)} THEN TRUE
                        WHEN {InvalidDateOperator._has_invalid_basic_date_components(value)} THEN TRUE
                        ELSE ({InvalidDateOperator._has_calendar_date_errors(value)})
                    END
                -- Any other format is invalid
                ELSE TRUE
            END
        """

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))

        def sql():
            return f"""
            CASE
                -- Empty values are invalid
                WHEN {self._is_empty_sql(target)} THEN TRUE
                ELSE ({self.invalid_date_sql(self._column_sql(target))})
            END
            """

        return self._do_check_operator(sql)
