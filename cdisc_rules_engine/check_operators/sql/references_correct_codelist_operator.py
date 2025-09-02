from .base_sql_operator import BaseSqlOperator


class ReferencesCorrectCodelistOperator(BaseSqlOperator):
    """Operator for checking if value references correct codelist."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        codelist = other_value.get("comparator")

        # Get the codelist terms for comparison
        if codelist in self.column_codelist_map:
            expected_terms = self.column_codelist_map[codelist]
        else:
            # Look up in codelist_term_maps if available
            expected_terms = []
            for term_map in self.codelist_term_maps:
                if term_map.get('codelist') == codelist:
                    expected_terms.extend(term_map.get('terms', []))

        # Check if target values are in the expected terms
        if expected_terms:
            results = self.validation_df[target].isin(expected_terms)
        else:
            # If no codelist found, all values are considered incorrect
            results = pd.Series([False] * len(self.validation_df))

        return results"""
        raise NotImplementedError("references_correct_codelist check_operator not implemented")
