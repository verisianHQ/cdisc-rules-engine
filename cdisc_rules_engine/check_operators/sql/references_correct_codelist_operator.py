from .base_sql_operator import BaseSqlOperator


class ReferencesCorrectCodelistOperator(BaseSqlOperator):
    """Operator for checking if value references correct codelist."""

    def execute_operator(self, other_value):
        raise NotImplementedError("references_correct_codelist check_operator not implemented")
