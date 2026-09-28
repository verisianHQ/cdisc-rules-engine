import pytest

from .helpers import (
    assert_operation_constant,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, target, expected",
    [
        ({"TIVERS": [None, None, None], "IETESTCD": ["A", "B", "C"]}, "TIVERS", True),
        ({"TIVERS": ["", "", ""], "IETESTCD": ["A", "B", "C"]}, "TIVERS", True),
        ({"TIVERS": [None, "", None], "IETESTCD": ["A", "B", "C"]}, "TIVERS", True),
        ({"TIVERS": [None, "2", None], "IETESTCD": ["A", "B", "C"]}, "TIVERS", False),
        ({"TIVERS": ["1", "1", "2"], "IETESTCD": ["A", "B", "C"]}, "TIVERS", False),
        ({"AESEQ": [None, None], "IETESTCD": ["A", "B"]}, "AESEQ", True),
        ({"AESEQ": [None, 2], "IETESTCD": ["A", "B"]}, "AESEQ", False),
        ({"AESEQ": [0, 0], "IETESTCD": ["A", "B"]}, "AESEQ", False),
        ({"IETESTCD": ["A", "B"]}, "TIVERS", True),
    ],
)
def test_variable_is_null(data, target, expected):
    operation = setup_sql_operations("variable_is_null", target, data)
    result = operation.execute()
    assert result.subtype == "Bool"
    assert_operation_constant(operation, result, expected)
