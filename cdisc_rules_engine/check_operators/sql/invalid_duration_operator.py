from .base_sql_operator import BaseSqlOperator


class InvalidDurationOperator(BaseSqlOperator):

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        target_column = self._column_sql(target)
        negative = other_value.get("negative", False)
        operation_column = f"{target}_invalid_duration".lower()

        def sql_subquery():
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
                    WHEN {target_column} IS NULL THEN false
                    WHEN {target_column}::text !~ '{pattern}' THEN true
                    WHEN {target_column}::text ~ 'T$' THEN true
                    WHEN {target_column}::text ~ '[.,].*[.,]' THEN true
                    WHEN {target_column}::text ~ '[.,]\\d*[YMDHMS][^S]*[.,]' THEN true
                    ELSE false
                END
            """

        return self._do_check_operator(operation_column, sql_subquery)
