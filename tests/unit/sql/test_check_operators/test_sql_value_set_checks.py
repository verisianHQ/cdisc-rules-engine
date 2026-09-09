import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import SqlOperatorError

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "target, comparator, expected_result",
    [
        ("BGSTRESU", "USUBJID", [False, False, True, True]),
        ("STRESU", "TESTCD", [True, True, True, False]),
        ("STRESU", ["TESTCD", "METHOD"], [False, False, False, False]),
        ("MISSING", "USUBJID", [False, False, False, False]),
        ("BGSTRESU", "MISSING", [False, False, False, False]),
        ("BGSTRESU", ["USUBJID", "MISSING"], [False, False, True, True]),
    ],
)
def test_sql_is_inconsistent_across_dataset(target, comparator, expected_result):
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ2", "SUBJ2"],
        "BGSTRESU": ["kg", "kg", "g", "mg"],
        "TESTCD": ["TEST1", "TEST1", "TEST1", "TEST2"],
        "METHOD": ["M1", "M1", "M2", "M2"],
        "SPEC": ["S1", "S1", "S1", "S1"],
        "STRESU": ["mg", "mg", "g", "kg"],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "target, comparator, expected_result",
    [
        ("BGSTRESU", "USUBJID", [False, False, True, True]),
        ("VSELTM", "VISITNUM", [True, True, True, True]),
        ("VSELTM", ["VISITNUM", "VSTPTNUM"], [True, True, True, True]),
    ],
)
def test_sql_is_inconsistent_across_dataset_with_nulls(target, comparator, expected_result):
    """Test case covering both NULL target values and NULL comparator values"""
    data = {
        "USUBJID": ["SUBJ1", "SUBJ1", "CDISC001", "CDISC001"],
        "BGSTRESU": ["kg", "kg", None, "g"],
        "VISITNUM": [4.0, 4.0, None, None],
        "VSELTM": ["-PT4H", "PT4H", "-PT4H", "PT4H"],
        "VSTPTNUM": [14, 14, 14, 14],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


def test_sql_is_inconsistent_across_dataset_where_populated():
    data = {
        "KEY": ["A", "A", "A", "B", "B", "B", ""],
        "VALUE": ["X", "Y", None, "Z", None, "", "Q"],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset({"target": "VALUE", "comparator": "KEY", "where_populated": True})
    assert_series_equals(result, [True, True, False, False, False, False, False])


def test_sql_is_inconsistent_across_dataset_where_populated_columns():
    data = {
        "KEY": ["A", "A", "A", "A", "B", "B"],
        "VALUE": ["X", "X", "Y", "X", "Z", "W"],
        "OTHER": ["p", "p", None, "p", "p", "p"],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset(
        {"target": "VALUE", "comparator": "KEY", "where_populated": ["OTHER"]}
    )
    assert_series_equals(result, [False, False, False, False, True, True])


def test_sql_is_inconsistent_across_dataset_where_populated_columns_multiple_comparators():
    data = {
        "DOMAIN": ["FT", "FT", "FT", "FT"],
        "VISITNUM": [201, 201, 201, 201],
        "ELTM": ["PT1H", "PT1H", "PT2H", "PT1H"],
        "TPT": ["90 MIN POST", "90 MIN POST", None, None],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset(
        {"target": "ELTM", "comparator": ["DOMAIN", "VISITNUM"], "where_populated": ["TPT"]}
    )
    assert_series_equals(result, [False, False, False, False])


def test_sql_is_inconsistent_across_dataset_where_populated_columns_missing_column():
    data = {
        "KEY": ["A", "A"],
        "VALUE": ["X", "Y"],
    }
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_inconsistent_across_dataset(
        {"target": "VALUE", "comparator": "KEY", "where_populated": ["MISSING"]}
    )
    assert_series_equals(result, [True, True])


def test_sql_is_inconsistent_across_dataset_where_populated_invalid_type():
    data = {
        "KEY": ["A", "A"],
        "VALUE": ["X", "Y"],
    }
    sql_ops = create_sql_operators(data)
    with pytest.raises(SqlOperatorError, match="Invalid where_populated type"):
        sql_ops.is_inconsistent_across_dataset({"target": "VALUE", "comparator": "KEY", "where_populated": "OTHER"})
