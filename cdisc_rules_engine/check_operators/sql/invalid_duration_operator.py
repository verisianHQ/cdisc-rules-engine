from .base_sql_operator import BaseSqlOperator


class InvalidDurationOperator(BaseSqlOperator):

    @staticmethod
    def invalid_duration_sql(value, negative: bool = False):
        """
        TRUE if the SQL expression `value` is not a valid ISO 8601 duration; NULL is not invalid.

        These regex patterns validate ISO 8601 duration format strings.
        They are custom implementations based on the ISO 8601 standard
        specifications for durations, which can be in the
        Format: P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W
        Originally implemented in business_rules.utils from the original engine.
        """
        if negative:
            pattern = (
                r"^[-]?P(?!$)(?:(?:(\d+(?:[.,]\d*)?Y)?[,]?(\d+(?:[.,]\d*)?M)?[,]?"
                r"(\d+(?:[.,]\d*)?D)?[,]?(T(?=\d)(?:(\d+(?:[.,]\d*)?H)?[,]?"
                r"(\d+(?:[.,]\d*)?M)?[,]?(\d+(?:[.,]\d*)?S)?)?)?)|"
                r"(\d+(?:[.,]\d*)?W))$"
            )
        else:
            pattern = (
                r"^P(?!$)(?:(?:(\d+(?:[.,]\d*)?Y)?[,]?(\d+(?:[.,]\d*)?M)?[,]?"
                r"(\d+(?:[.,]\d*)?D)?[,]?(T(?=\d)(?:(\d+(?:[.,]\d*)?H)?[,]?"
                r"(\d+(?:[.,]\d*)?M)?[,]?(\d+(?:[.,]\d*)?S)?)?)?)|"
                r"(\d+(?:[.,]\d*)?W))$"
            )
        return f"""
            CASE
                WHEN {value} IS NULL THEN false
                WHEN {value}::text !~ '{pattern}' THEN true
                WHEN {value}::text ~ 'T$' THEN true
                WHEN {value}::text ~ '[.,].*[.,]' THEN true
                WHEN {value}::text ~ '[.,]\\d*[YMDHMS][^S]*[.,]' THEN true
                ELSE false
            END
        """

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        target_column = self._column_sql(target)
        negative = other_value.get("negative", False)

        return self._do_check_operator(lambda: self.invalid_duration_sql(target_column, negative))
