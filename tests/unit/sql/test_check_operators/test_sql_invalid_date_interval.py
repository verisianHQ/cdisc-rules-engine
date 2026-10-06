import pytest

from .helpers import assert_series_equals, create_sql_operators


@pytest.mark.parametrize(
    "value, expected",
    [
        # start datetime / end datetime
        ("2023-01-01T08:00:00/2023-01-02T08:00:00", False),
        ("2023-01-01/2023-01-02", False),
        ("2023-01/2023-02", False),
        ("2023-01-01T08:00:00Z/2023-01-01T09:00:00Z", False),
        # start datetime / duration
        ("2023-01-01T08:00:00/P1DT2H", False),
        ("2023-01-01/P1Y2M3D", False),
        ("2023-01-01T08:00/PT1.5H", False),
        ("2023-01-01/P2W", False),
        # duration / end datetime
        ("P1DT2H/2023-01-02T08:00:00", False),
        ("PT30M/2023-01-02", False),
        # empty
        (None, True),
        ("", True),
        # not an interval
        ("2023-01-01", True),
        ("P1D", True),
        # wrong shape
        ("2023-01-01/", True),
        ("/2023-01-01", True),
        ("2023-01-01/2023-01-02/2023-01-03", True),
        ("2023-01-01 / 2023-01-02", True),
        # duration / duration
        ("P1D/P2D", True),
        # invalid datetime part
        ("2023-13-01/2023-01-02", True),
        ("2023-01-01/2023-02-30", True),
        ("2023-01-01T25:00/P1D", True),
        ("2023-01-/P1D", True),
        ("P1D/invalid", True),
        # invalid duration part
        ("2023-01-01/P-1D", True),
        ("2023-01-01/PT", True),
        ("2023-01-01/1D", True),
        ("2023-01-01T08:00:00/P1DT", True),
    ],
)
def test_sql_invalid_date_interval(value, expected):
    sql_ops = create_sql_operators({"target": [value]})
    result = sql_ops.invalid_date_interval({"target": "target"})
    assert_series_equals(result, [expected])
