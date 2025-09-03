from business_rules.fields import FIELD_DATAFRAME
from business_rules.operators import BaseType, type_operator

from cdisc_rules_engine.check_operators.sql.not_operator import NotOperator

from .base_sql_operator import log_operator_execution
from .exists_operator import ExistsOperator
from .not_exists_operator import NotExistsOperator
from .equal_to_operator import EqualToOperator
from .not_equal_to_operator import NotEqualToOperator
from .equal_to_case_insensitive_operator import EqualToCaseInsensitiveOperator
from .not_equal_to_case_insensitive_operator import NotEqualToCaseInsensitiveOperator
from .empty_operator import EmptyOperator
from .less_than_operator import LessThanOperator
from .greater_than_operator import GreaterThanOperator
from .less_than_or_equal_to_operator import LessThanOrEqualToOperator
from .greater_than_or_equal_to_operator import GreaterThanOrEqualToOperator
from .is_contained_by_operator import IsContainedByOperator
from .has_different_values_operator import HasDifferentValuesOperator
from .date_equal_to_operator import DateEqualToOperator
from .date_not_equal_to_operator import DateNotEqualToOperator
from .date_less_than_operator import DateLessThanOperator
from .date_less_than_or_equal_to_operator import DateLessThanOrEqualToOperator
from .date_greater_than_operator import DateGreaterThanOperator
from .date_greater_than_or_equal_to_operator import DateGreaterThanOrEqualToOperator
from .is_contained_by_case_insensitive_operator import IsContainedByCaseInsensitiveOperator
from .is_not_unique_relationship_operator import IsNotUniqueRelationshipOperator
from .present_on_multiple_rows_within_operator import PresentOnMultipleRowsWithinOperator
from .prefix_is_contained_by_operator import PrefixIsContainedByOperator
from .suffix_is_contained_by_operator import SuffixIsContainedByOperator
from .contains_operator import ContainsOperator
from .contains_case_insensitive_operator import ContainsCaseInsensitiveOperator
from .matches_regex_operator import MatchesRegexOperator
from .not_matches_regex_operator import NotMatchesRegexOperator
from .prefix_matches_regex_operator import PrefixMatchesRegexOperator
from .not_prefix_matches_regex_operator import NotPrefixMatchesRegexOperator
from .suffix_matches_regex_operator import SuffixMatchesRegexOperator
from .not_suffix_matches_regex_operator import NotSuffixMatchesRegexOperator
from .starts_with_operator import StartsWithOperator
from .ends_with_operator import EndsWithOperator
from .equals_string_part_operator import EqualsStringPartOperator
from .invalid_date_operator import InvalidDateOperator
from .invalid_duration_operator import InvalidDurationOperator
from .is_complete_date_operator import IsCompleteDateOperator
from .is_unique_set_operator import IsUniqueSetOperator
from .is_ordered_set_operator import IsOrderedSetOperator
from .is_inconsistent_across_dataset_operator import IsInconsistentAcrossDatasetOperator
from .conformant_value_data_type_operator import ConformantValueDataTypeOperator
from .conformant_value_length_operator import ConformantValueLengthOperator
from .suffix_equal_to_operator import SuffixEqualToOperator
from .prefix_equal_to_operator import PrefixEqualToOperator
from .has_equal_length_operator import HasEqualLengthOperator
from .longer_than_operator import LongerThanOperator
from .longer_than_or_equal_to_operator import LongerThanOrEqualToOperator
from .empty_within_except_last_row_operator import EmptyWithinExceptLastRowOperator
from .contains_all_operator import ContainsAllOperator
from .has_next_corresponding_record_operator import HasNextCorrespondingRecordOperator
from .inconsistent_enumerated_columns_operator import InconsistentEnumeratedColumnsOperator
from .references_correct_codelist_operator import ReferencesCorrectCodelistOperator
from .is_ordered_by_operator import IsOrderedByOperator
from .value_has_multiple_references_operator import ValueHasMultipleReferencesOperator
from .target_is_sorted_by_operator import TargetIsSortedByOperator
from .variable_metadata_equal_to_operator import VariableMetadataEqualToOperator
from .shares_at_least_one_element_with_operator import SharesAtLeastOneElementWithOperator
from .shares_exactly_one_element_with_operator import SharesExactlyOneElementWithOperator
from .shares_no_elements_with_operator import SharesNoElementsWithOperator
from .is_ordered_subset_of_operator import IsOrderedSubsetOfOperator


class PostgresQLOperators(BaseType):
    """
    Main SQL operators class with dynamic method registration.

    This class uses dynamic registration to combine functionality from individual
    operator classes, maintaining compatibility with the business rules framework
    while providing operations-like modularity.
    """

    name = "dataframe"

    _operator_map = {
        "exists": ExistsOperator,
        "not_exists": NotExistsOperator,  # TODO
        "equal_to": EqualToOperator,
        "not_equal_to": NotEqualToOperator,  # TODO 1 single class for equalities
        "equal_to_case_insensitive": EqualToCaseInsensitiveOperator,
        "not_equal_to_case_insensitive": NotEqualToCaseInsensitiveOperator,
        "empty": EmptyOperator,
        "non_empty": (NotOperator, EmptyOperator),
        "less_than": LessThanOperator,  # TODO 1 single class for comparisons
        "greater_than": GreaterThanOperator,
        "less_than_or_equal_to": LessThanOrEqualToOperator,
        "greater_than_or_equal_to": GreaterThanOrEqualToOperator,
        "is_contained_by": IsContainedByOperator,
        "is_not_contained_by": (NotOperator, IsContainedByOperator),
        "is_contained_by_case_insensitive": IsContainedByCaseInsensitiveOperator,
        "is_not_contained_by_case_insensitive": (NotOperator, IsContainedByCaseInsensitiveOperator),
        "has_different_values": HasDifferentValuesOperator,
        "has_same_values": (NotOperator, HasDifferentValuesOperator),
        "date_equal_to": DateEqualToOperator,  # TODO 1 single class for dates
        "date_not_equal_to": DateNotEqualToOperator,
        "date_less_than": DateLessThanOperator,
        "date_less_than_or_equal_to": DateLessThanOrEqualToOperator,
        "date_greater_than": DateGreaterThanOperator,
        "date_greater_than_or_equal_to": DateGreaterThanOrEqualToOperator,
        "is_not_unique_relationship": IsNotUniqueRelationshipOperator,
        "is_unique_relationship": (NotOperator, IsNotUniqueRelationshipOperator),
        "present_on_multiple_rows_within": PresentOnMultipleRowsWithinOperator,
        "not_present_on_multiple_rows_within": (NotOperator, PresentOnMultipleRowsWithinOperator),
        "prefix_is_contained_by": PrefixIsContainedByOperator,
        "prefix_is_not_contained_by": (NotOperator, PrefixIsContainedByOperator),
        "suffix_is_contained_by": SuffixIsContainedByOperator,
        "suffix_is_not_contained_by": (NotOperator, SuffixIsContainedByOperator),
        "contains": ContainsOperator,
        "does_not_contain": (NotOperator, ContainsOperator),
        "contains_case_insensitive": ContainsCaseInsensitiveOperator,
        "does_not_contain_case_insensitive": (NotOperator, ContainsCaseInsensitiveOperator),
        "matches_regex": MatchesRegexOperator,
        "not_matches_regex": NotMatchesRegexOperator,  # TODO check if this can use Not Operator
        "prefix_matches_regex": PrefixMatchesRegexOperator,
        "not_prefix_matches_regex": NotPrefixMatchesRegexOperator,
        "suffix_matches_regex": SuffixMatchesRegexOperator,
        "not_suffix_matches_regex": NotSuffixMatchesRegexOperator,
        "starts_with": StartsWithOperator,
        "ends_with": EndsWithOperator,
        "equals_string_part": EqualsStringPartOperator,
        "does_not_equal_string_part": (NotOperator, EqualsStringPartOperator),
        "invalid_date": InvalidDateOperator,
        "invalid_duration": InvalidDurationOperator,
        "is_complete_date": IsCompleteDateOperator,
        "is_incomplete_date": (NotOperator, IsCompleteDateOperator),
        "is_unique_set": IsUniqueSetOperator,
        "is_not_unique_set": (NotOperator, IsUniqueSetOperator),
        "is_ordered_set": IsOrderedSetOperator,
        "is_not_ordered_set": (NotOperator, IsOrderedByOperator),
        "is_inconsistent_across_dataset": IsInconsistentAcrossDatasetOperator,
        "conformant_value_data_type": ConformantValueDataTypeOperator,
        "non_conformant_value_data_type": (NotOperator, ConformantValueDataTypeOperator),
        "conformant_value_length": ConformantValueLengthOperator,
        "non_conformant_value_length": (NotOperator, ConformantValueLengthOperator),
        "suffix_equal_to": SuffixEqualToOperator,
        "suffix_not_equal_to": (NotOperator, SuffixEqualToOperator),
        "prefix_equal_to": PrefixEqualToOperator,
        "prefix_not_equal_to": (NotOperator, PrefixEqualToOperator),
        "has_equal_length": HasEqualLengthOperator,
        "has_not_equal_length": (NotOperator, HasEqualLengthOperator),
        "longer_than": LongerThanOperator,
        "shorter_than_or_equal_to": (NotOperator, LongerThanOperator),
        "longer_than_or_equal_to": LongerThanOrEqualToOperator,
        "shorter_than": (NotOperator, LongerThanOrEqualToOperator),
        "empty_within_except_last_row": EmptyWithinExceptLastRowOperator,
        "non_empty_within_except_last_row": (NotOperator, EmptyWithinExceptLastRowOperator),
        "contains_all": ContainsAllOperator,
        "not_contains_all": (NotOperator, ContainsAllOperator),
        "has_next_corresponding_record": HasNextCorrespondingRecordOperator,
        "does_not_have_next_corresponding_record": (NotOperator, HasNextCorrespondingRecordOperator),
        "inconsistent_enumerated_columns": InconsistentEnumeratedColumnsOperator,
        "references_correct_codelist": ReferencesCorrectCodelistOperator,
        "does_not_reference_correct_codelist": (NotOperator, ReferencesCorrectCodelistOperator),
        "is_ordered_by": IsOrderedByOperator,
        "is_not_ordered_by": (NotOperator, IsOrderedByOperator),
        "value_has_multiple_references": ValueHasMultipleReferencesOperator,
        "value_does_not_have_multiple_references": (NotOperator, ValueHasMultipleReferencesOperator),
        "target_is_sorted_by": TargetIsSortedByOperator,
        "target_is_not_sorted_by": (NotOperator, TargetIsSortedByOperator),
        "variable_metadata_equal_to": VariableMetadataEqualToOperator,
        "variable_metadata_not_equal_to": (NotOperator, VariableMetadataEqualToOperator),
        "shares_at_least_one_element_with": SharesAtLeastOneElementWithOperator,
        "shares_exactly_one_element_with": SharesExactlyOneElementWithOperator,
        "shares_no_elements_with": SharesNoElementsWithOperator,
        "is_ordered_subset_of": IsOrderedSubsetOfOperator,
        "is_not_ordered_subset_of": (NotOperator, IsOrderedSubsetOfOperator),
    }

    def __init__(self, data):
        self.data = data

    def __getattr__(self, name):
        """
        Dynamically create and cache an operator method on its first access.
        Handles both simple operators and wrapped operators.
        """
        if name in self._operator_map:
            recipe = self._operator_map[name]
            operator_instance = None

            if isinstance(recipe, tuple):
                # (NotOperator, Operator)
                WrapperClass, WrappedClass = recipe
                operator_instance = WrapperClass(self.data, WrappedClass)
            else:
                operator_class = recipe
                operator_instance = operator_class(self.data)

            # Define the method with the necessary decorators
            @log_operator_execution
            @type_operator(FIELD_DATAFRAME)
            def operator_method(self, other_value):
                return operator_instance.execute_operator(other_value)

            # Cache the new method on the instance
            bound_method = operator_method.__get__(self, type(self))
            setattr(self, name, bound_method)
            return bound_method

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _assert_valid_value_and_cast(self, value):
        """Shared method for value validation and casting."""
        return value
