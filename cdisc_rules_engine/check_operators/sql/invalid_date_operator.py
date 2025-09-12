from .base_sql_operator import BaseSqlOperator


class InvalidDateOperator(BaseSqlOperator):
    """
    Operator for checking if date is invalid.

    This implementation matches the business_rules.utils.is_valid_date logic:
    - Simple years (YYYY) are valid if reasonable (1-9999)
    - Partial dates (YYYY-MM) are valid with basic validation
    - Full ISO dates are valid if they can be parsed as timestamps
    - Malformed formats are invalid
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
                        -- Handle 4-digit year format only (YYYY) - original only accepts 4-digit years
                        WHEN {self._column_sql(target)} ~ '^-?[0-9]{{4}}$' THEN FALSE
                        -- Handle partial date format (YYYY-MM) - basic validation
                        WHEN {self._column_sql(target)} ~ '^-?[0-9]{{4}}-[0-9]{{2}}$' THEN
                            CASE
                                WHEN CAST(RIGHT({self._column_sql(target)}, 2) AS INTEGER) > 12
                                     OR CAST(RIGHT({self._column_sql(target)}, 2) AS INTEGER) < 1
                                     THEN TRUE
                                ELSE FALSE
                            END
                        -- Handle uncertainty patterns with double dashes - only -- patterns are valid
                        WHEN {self._column_sql(target)} ~ '^-?[0-9]{{4}}--$'
                             OR {self._column_sql(target)} ~ '^-?[0-9]{{4}}-[0-9]{{2}}--$'
                             THEN FALSE
                        -- Handle complete ISO date format - validate calendar dates properly
                        WHEN {self._column_sql(target)} ~
                             '^-?[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}(T[0-9]{{2}}:[0-9]{{2}}(:[0-9]{{2}})?(\\.[0-9]+)?([+-][0-9]{{2}}:?[0-9]{{2}}|Z)?)?$'
                             THEN
                            CASE
                                -- Check for obvious invalid time components
                                WHEN {self._column_sql(target)} ~ 'T(2[4-9]|[3-9][0-9]):' THEN TRUE
                                -- Invalid seconds/minutes (>=60)
                                WHEN {self._column_sql(target)} ~ ':([6-9][0-9])([^0-9]|$)' THEN TRUE
                                -- Check for basic date component validity (month 01-12, day 01-31)
                                WHEN CAST(SUBSTRING({self._column_sql(target)}, 6, 2) AS INTEGER) > 12
                                     OR CAST(SUBSTRING({self._column_sql(target)}, 6, 2) AS INTEGER) < 1
                                     THEN TRUE
                                WHEN CAST(SUBSTRING({self._column_sql(target)}, 9, 2) AS INTEGER) > 31
                                     OR CAST(SUBSTRING({self._column_sql(target)}, 9, 2) AS INTEGER) < 1
                                     THEN TRUE
                                -- Use a more robust date validation that catches calendar errors
                                ELSE (
                                    -- Extract date part only for validation
                                    SELECT CASE
                                        -- Feb 29 in non-leap years (proper leap year calculation)
                                        WHEN SUBSTRING({self._column_sql(target)}, 6, 5) = '02-29'
                                         AND NOT (
                                             (CAST(SUBSTRING({self._column_sql(target)}, 1, 4) AS INTEGER) % 4 = 0
                                              AND CAST(SUBSTRING({self._column_sql(target)}, 1, 4) AS INTEGER)
                                                  % 100 != 0)
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
                                )
                            END
                        -- Any other format is invalid (malformed datetime strings, wrong separators, etc.)
                        ELSE TRUE
                    END
                )
            END
            """

        return self._do_check_operator(op_name, sql)
