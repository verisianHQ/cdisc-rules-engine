from unittest.mock import patch

import pytest

from cdisc_rules_engine.constants.classes import INTERVENTIONS

from .helpers import (
    assert_operation_collection,
    setup_sql_operations,
)

test_data = {
    "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
    "AETERM": ["test", "test", "test"],
}

mock_metadata = [
    {
        "name": "STUDYID",
        "role": "Identifier",
        "ordinal": 1,
    },
    {
        "name": "DOMAIN",
        "role": "Identifier",
        "ordinal": 2,
    },
    {
        "name": "USUBJID",
        "role": "Identifier",
        "ordinal": 3,
    },
    {
        "name": "--TERM",
        "role": "Topic",
        "ordinal": 4,
    },
    {
        "name": "VISITNUM",
        "role": "Timing",
        "ordinal": 17,
    },
    {
        "name": "VISIT",
        "role": "Timing",
        "ordinal": 18,
    },
]


def test_get_column_order_from_library():
    """The full ordinal-sorted variable list is returned, with -- replaced by the domain."""
    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with patch.object(operation, "_get_variables_metadata_from_standard", return_value=mock_metadata):
        result = operation.execute()
        assert_operation_collection(
            operation,
            result,
            ["STUDYID", "DOMAIN", "USUBJID", "test_tableTERM", "VISITNUM", "VISIT"],
        )


@pytest.mark.parametrize(
    "key_name, key_value, expected",
    [
        ("role", "Timing", ["VISITNUM", "VISIT"]),
        ("role", "Identifier", ["STUDYID", "DOMAIN", "USUBJID"]),
        ("role", "NonExistentRole", []),
        ("role", "", ["STUDYID", "DOMAIN", "USUBJID", "test_tableTERM", "VISITNUM", "VISIT"]),
        (None, None, ["STUDYID", "DOMAIN", "USUBJID", "test_tableTERM", "VISITNUM", "VISIT"]),
    ],
)
def test_get_column_order_from_library_with_filter(key_name, key_value, expected):
    """key_name/key_value optionally filter the variables before names are extracted."""
    operation = setup_sql_operations(
        "get_column_order_from_library",
        None,
        test_data,
        standards_context="sdtm",
        extra_config={"key_name": key_name, "key_value": key_value},
    )

    with patch.object(operation, "_get_variables_metadata_from_standard", return_value=mock_metadata):
        result = operation.execute()
        assert_operation_collection(operation, result, expected)


def test_get_column_order_from_library_deduplicates_preserving_order():
    """Duplicate variable names (e.g. present in both model and IG metadata) are deduplicated."""
    duplicated_metadata = [
        {"name": "STUDYID", "ordinal": 1},
        {"name": "DOMAIN", "ordinal": 2},
        {"name": "STUDYID", "ordinal": 3},
    ]
    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with patch.object(operation, "_get_variables_metadata_from_standard", return_value=duplicated_metadata):
        result = operation.execute()
        assert_operation_collection(operation, result, ["STUDYID", "DOMAIN"])


def test_get_column_order_from_library_merges_model_only_variables():
    """
    A domain's own IG spec doesn't always re-list every variable defined at the model level
    (e.g. a generic Timing variable like --DTC). For detectable classes (Interventions,
    Events, Findings, Findings About), those should be merged in - at the model's own
    relative position (here, before --STDTC/--ENDTC) - rather than silently dropped or
    tacked onto the end, while variables the IG already defines keep their IG data.
    """
    ig_metadata = [
        {"name": "STUDYID", "role": "Identifier", "ordinal": 1},
        {"name": "DOMAIN", "role": "Identifier", "ordinal": 2},
        {"name": "USUBJID", "role": "Identifier", "ordinal": 3},
        {"name": "--TRT", "role": "Topic", "ordinal": 4},
        {"name": "--STDTC", "role": "Timing", "ordinal": 5},
        {"name": "--ENDTC", "role": "Timing", "ordinal": 6},
    ]
    model_metadata = [
        {"name": "STUDYID", "role": "Identifier", "ordinal": 1},
        {"name": "DOMAIN", "role": "Identifier", "ordinal": 2},
        {"name": "USUBJID", "role": "Identifier", "ordinal": 3},
        {"name": "--TRT", "role": "Topic", "ordinal": 4},
        {"name": "--DTC", "role": "Timing", "ordinal": 30},  # model-only: missing from the IG spec above
        {"name": "--STDTC", "role": "Timing", "ordinal": 31},
        {"name": "--ENDTC", "role": "Timing", "ordinal": 32},
    ]

    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with (
        patch.object(operation, "_get_variables_metadata_from_standard", return_value=ig_metadata),
        patch.object(operation, "_get_variables_metadata_from_standard_model", return_value=model_metadata),
        patch.object(operation, "get_dataset_class", return_value=INTERVENTIONS),
    ):
        result = operation.execute()
        assert_operation_collection(
            operation,
            result,
            ["STUDYID", "DOMAIN", "USUBJID", "test_tableTRT", "test_tableDTC", "test_tableSTDTC", "test_tableENDTC"],
        )


def test_get_column_order_from_library_inserts_ig_only_extra_variable():
    """
    A variable the IG defines that the model class template has no equivalent for at all
    (a genuinely domain-specific extra, not just a differently-templated name) is inserted
    by role rather than dropped: Identifiers near the front, Timing at the end, everything
    else just before the Timing tail.
    """
    ig_metadata = [
        {"name": "STUDYID", "role": "Identifier", "ordinal": 1},
        {"name": "DOMAIN", "role": "Identifier", "ordinal": 2},
        {"name": "--TRT", "role": "Topic", "ordinal": 3},
        {"name": "--CLAS", "role": "Qualifier", "ordinal": 4},  # IG-only: no model equivalent
        {"name": "--STDTC", "role": "Timing", "ordinal": 5},
    ]
    model_metadata = [
        {"name": "STUDYID", "role": "Identifier", "ordinal": 1},
        {"name": "DOMAIN", "role": "Identifier", "ordinal": 2},
        {"name": "--TRT", "role": "Topic", "ordinal": 3},
        {"name": "--STDTC", "role": "Timing", "ordinal": 30},
    ]

    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with (
        patch.object(operation, "_get_variables_metadata_from_standard", return_value=ig_metadata),
        patch.object(operation, "_get_variables_metadata_from_standard_model", return_value=model_metadata),
        patch.object(operation, "get_dataset_class", return_value=INTERVENTIONS),
    ):
        result = operation.execute()
        assert_operation_collection(
            operation,
            result,
            ["STUDYID", "DOMAIN", "test_tableTRT", "test_tableCLAS", "test_tableSTDTC"],
        )


def test_get_column_order_from_library_skips_model_merge_for_non_detectable_class():
    """Non-detectable classes (e.g. RELATIONSHIP) use the IG list as-is, unmerged."""
    ig_metadata = [
        {"name": "STUDYID", "role": "Identifier", "ordinal": 1},
        {"name": "RDOMAIN", "role": "Identifier", "ordinal": 2},
    ]
    model_metadata = [
        {"name": "--DTC", "role": "Timing", "ordinal": 30},
    ]

    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with (
        patch.object(operation, "_get_variables_metadata_from_standard", return_value=ig_metadata),
        patch.object(operation, "_get_variables_metadata_from_standard_model", return_value=model_metadata),
        patch.object(operation, "get_dataset_class", return_value="RELATIONSHIP"),
    ):
        result = operation.execute()
        assert_operation_collection(operation, result, ["STUDYID", "RDOMAIN"])


def test_get_column_order_from_library_exception_handling():
    """Metadata retrieval failures should raise, since the rule can't run without it."""
    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with patch.object(
        operation, "_get_variables_metadata_from_standard", side_effect=Exception("Metadata retrieval failed")
    ):
        with pytest.raises(Exception, match="Metadata retrieval failed"):
            operation.execute()
