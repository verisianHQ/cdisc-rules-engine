import re
from .base_sql_operator import BaseSqlOperator


class HasPreviousEnumeratedColumnOperator(BaseSqlOperator):
    """
    Checks if the chronologically previous enumerated column exists.
    """

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target"))

        if not isinstance(target_column, str):
            raise TypeError(f"Expected a target column string, got {type(target_column).__name__}: {target_column}.")

        match = re.search(r"(\d+)(?=\D*$)", target_column)
        if not match:
            raise ValueError(f"Column '{target_column}' does not contain an enumerated number.")

        num_str = match.group(1)
        num = int(num_str)

        if num == 1:
            return self._do_check_operator(lambda: "TRUE")

        prev_num_str = str(num - 1).zfill(len(num_str))
        prev_column = target_column[: match.start()] + prev_num_str + target_column[match.end() :]

        if self._exists(prev_column.lower()):
            return self._do_check_operator(lambda: "TRUE")
        else:
            return self._do_check_operator(lambda: "FALSE")
