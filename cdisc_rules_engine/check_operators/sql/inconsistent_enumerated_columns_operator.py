from .base_sql_operator import BaseSqlOperator


class InconsistentEnumeratedColumnsOperator(BaseSqlOperator):
    """Operator for checking inconsistent enumerated columns."""

    def execute_operator(self, other_value):
        """target_columns = other_value.get("target")
        if not isinstance(target_columns, list):
            target_columns = [target_columns]

        replaced_columns = [self.replace_prefix(col) for col in target_columns]

        # For inconsistent enumerated columns, we need to check if the enumerated values
        # across different columns are consistent
        results = []
        for _, row in self.validation_df.iterrows():
            row_values = [row[col] for col in replaced_columns if col in row]
            # Check if values follow expected enumeration pattern
            is_consistent = len(set(row_values)) <= 1 if row_values else True
            results.append(not is_consistent)

        return pd.Series(results)"""
        raise NotImplementedError("inconsistent_enumerated_columns check_operator not implemented")
