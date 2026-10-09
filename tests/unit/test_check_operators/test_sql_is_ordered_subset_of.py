import pytest
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from .helpers import assert_series_equals, create_sql_operators

mock_op_variables = {
    "$model_columns": SqlOperationResult(
        query="SELECT val FROM (VALUES ('STUDYID'), ('USUBJID'), ('AESEQ'), ('AETERM')) AS t(val)",
        type="collection",
        subtype="Char",
    ),
    "$valid_dataset_columns": SqlOperationResult(
        query="SELECT val FROM (VALUES ('STUDYID'), ('AESEQ'), ('AETERM')) AS t(val)",
        type="collection",
        subtype="Char",
    ),
    "$invalid_dataset_columns": SqlOperationResult(
        query="SELECT val FROM (VALUES ('STUDYID'), ('AETERM'), ('AESEQ')) AS t(val)",
        type="collection",
        subtype="Char",
    ),
}

is_ordered_subset_test_data = [
    (
        {"target": ["A", "C", "E"]},
        "target",
        ["A", "B", "C", "D", "E"],
        [True, True, True],
    ),
    (
        {"target": ["C", "A", "E"]},
        "target",
        ["A", "B", "C", "D", "E"],
        [False, False, False],
    ),
    (
        {"target": ["A", "X", "E"]},
        "target",
        ["A", "B", "C", "D", "E"],
        [False, False, False],
    ),
    (
        {"target": ["A", "B", "C"]},
        "target",
        ["A", "B", "C"],
        [True, True, True],
    ),
    (
        {"dummy": ["1", "2"]},
        "$valid_dataset_columns",
        "$model_columns",
        [True, True],
    ),
    (
        {"dummy": ["1", "2"]},
        "$invalid_dataset_columns",
        "$model_columns",
        [False, False],
    ),
    (
        {"dummy": ["1"]},
        ["USUBJID", "AETERM"],
        ["STUDYID", "USUBJID", "AESEQ", "AETERM"],
        [True],
    ),
    (
        {"dummy": ["1"]},
        ["AETERM", "USUBJID"],
        ["STUDYID", "USUBJID", "AESEQ", "AETERM"],
        [False],
    ),
]

is_not_ordered_subset_test_data = [
    (
        {"target": ["A", "C", "E"]},
        "target",
        ["A", "B", "C", "D", "E"],
        [False, False, False],
    ),
    (
        {"target": ["C", "A", "E"]},
        "target",
        ["A", "B", "C", "D", "E"],
        [True, True, True],
    ),
    (
        {"dummy": ["1", "2"]},
        "$valid_dataset_columns",
        "$model_columns",
        [False, False],
    ),
    (
        {"dummy": ["1", "2"]},
        "$invalid_dataset_columns",
        "$model_columns",
        [True, True],
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    is_ordered_subset_test_data,
)
def test_is_ordered_subset_of(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data, extra_operation_variables=mock_op_variables)
    result = sql_ops.is_ordered_subset_of(
        {
            "target": target,
            "comparator": comparator,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    is_not_ordered_subset_test_data,
)
def test_is_not_ordered_subset_of(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data, extra_operation_variables=mock_op_variables)
    result = sql_ops.is_not_ordered_subset_of(
        {
            "target": target,
            "comparator": comparator,
        }
    )
    assert_series_equals(result, expected_result)
