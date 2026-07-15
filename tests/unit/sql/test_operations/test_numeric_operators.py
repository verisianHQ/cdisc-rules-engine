import pytest

from .helpers import (
    assert_operation_constant,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, op, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, "max", 18),
        ({"values": [11, 12, 12, 5, 18, 9]}, "min", 5),
        ({"values": [11, 12, 12, 5, 17, 9]}, "mean", 11),
    ],
)
def test_sql_maximum(data, op, expected):
    operation = setup_sql_operations(op, "values", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, op, expected",
    [
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            "max",
            [12.0, 12.0, 12.0, 18.0, 18.0, 9.0],
        ),
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            "min",
            [11.0, 11.0, 11.0, 5.0, 5.0, 9.0],
        ),
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 17, 9]},
            "mean",
            [11.666666666666666, 11.666666666666666, 11.666666666666666, 11.0, 11.0, 9.0],
        ),
    ],
)
def test_sql_maximum_grouping(data, op, expected):
    operation = setup_sql_operations(op, "values", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()

    assert result.type == "window"
    operation.data_service.pgi.execute_sql(result.query)
    query_results = operation.data_service.pgi.fetch_all()
    query_results.sort(key=lambda x: x["id"])
    actual_values = [float(row["value"]) for row in query_results]

    assert actual_values == pytest.approx(expected)
