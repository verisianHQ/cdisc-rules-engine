import pytest
from cdisc_rules_engine.check_operators.sql.has_previous_enumerated_column_operator import (
    HasPreviousEnumeratedColumnOperator,
)
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "data, target, expected",
    [
        (
            {"STUDYID": ["1", "2"], "TRT01P": ["A", "B"]},
            "TRT01P",
            [True, True],
        ),
        (
            {"STUDYID": ["1", "2"], "PARAM1": ["A", "B"]},
            "PARAM1",
            [True, True],
        ),
        (
            {"STUDYID": ["1", "2"], "TRT01P": ["A", "B"], "TRT02P": ["C", "D"]},
            "TRT02P",
            [True, True],
        ),
        (
            {"STUDYID": ["1", "2"], "PARAM1": ["A", "B"], "PARAM2": ["C", "D"]},
            "PARAM2",
            [True, True],
        ),
        (
            {"STUDYID": ["1", "2"], "TRT01P": ["A", "B"], "TRT03P": ["C", "D"]},
            "TRT03P",
            [False, False],
        ),
        (
            {"STUDYID": ["1", "2"], "PARAM2": ["C", "D"]},
            "PARAM2",
            [False, False],
        ),
    ],
)
def test_has_previous_enumerated_column(data, target, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="ADSL",
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    operator_data = {
        "data_service": data_service,
        "dataset_id": "ADSL",
    }

    operator = HasPreviousEnumeratedColumnOperator(operator_data)
    result = operator.execute_operator({"target": target})

    assert result.tolist() == expected


@pytest.mark.parametrize(
    "data, target",
    [
        ({"STUDYID": ["1", "2"]}, 123),
        ({"STUDYID": ["1", "2"]}, None),
    ],
)
def test_has_previous_enumerated_column_type_error(data, target, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="ADSL",
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    operator_data = {
        "data_service": data_service,
        "dataset_id": "ADSL",
    }

    operator = HasPreviousEnumeratedColumnOperator(operator_data)
    with pytest.raises(TypeError, match="Expected a target column string"):
        operator.execute_operator({"target": target})


@pytest.mark.parametrize(
    "data, target",
    [
        ({"STUDYID": ["1", "2"]}, "STUDYID"),
        ({"STUDYID": ["1", "2"], "TRTP": ["A", "B"]}, "TRTP"),
    ],
)
def test_has_previous_enumerated_column_value_error(data, target, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="ADSL",
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    operator_data = {
        "data_service": data_service,
        "dataset_id": "ADSL",
    }

    operator = HasPreviousEnumeratedColumnOperator(operator_data)
    with pytest.raises(ValueError, match="does not contain an enumerated number"):
        operator.execute_operator({"target": target})
