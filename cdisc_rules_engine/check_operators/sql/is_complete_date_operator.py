from .base_sql_operator import BaseSqlOperator
from business_rules.utils import vectorized_is_complete_date


class IsCompleteDateOperator(BaseSqlOperator):
    """Operator for checking if date is complete."""

    def execute_operator(self, other_value):
        # This operator has some implementation in the original version
        target = self.replace_prefix(other_value.get("target"))
        results = vectorized_is_complete_date(self.validation_df[target])
        return self.validation_df.convert_to_series(results)
