import pytest

from .helpers import (
    assert_operation_constant,
    assert_operation_parameterized_constant,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, target, expected",
    [
        (
            {
                "STUDYID": ["CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE"],
                "EQ": [1, 2],
                "values": ["TEST1", "TEST1"],
            },
            "values",
            2,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE"],
                "EQ": [1, 2],
                "values": ["TEST1", "TEST1"],
            },
            None,
            2,
        ),
    ],
)
def test_record_count(data, target, expected):
    operation = setup_sql_operations("record_count", target, data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, filter, expected",
    [
        (
            {
                "STUDYID": ["CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE"],
                "EQ": [1, 2],
                "values": ["TEST1", "TEST1"],
            },
            {"STUDYID": "CDISC02"},
            1,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE"],
                "EQ": [1, 2],
                "values": ["TEST1", "TEST1"],
            },
            {"STUDYID": "CDISC03"},
            0,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02", "CDISC03"],
                "DOMAIN": ["AE", "AE", "DM"],
                "EQ": [1, 2, 3],
                "values": ["TEST1", "TEST1", "TEST1"],
            },
            {"DOMAIN": "AE", "EQ": 2},
            1,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02", "CDISC03"],
                "DOMAIN": ["AE", "AE", "DM"],
                "EQ": [1, 2, 3],
                "USUBJID": ["TEST1", "TEST2", "ABC"],
                "values": ["TEST1", "TEST1", "TEST1"],
            },
            {"USUBJID": "TEST%"},
            2,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02", "CDISC03"],
                "DOMAIN": ["AE", "AE", "AE"],
                "USUBJID": ["A_1", "AB1", "A_2"],
                "values": ["TEST1", "TEST1", "TEST1"],
            },
            {"USUBJID": "A_%"},
            2,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02", "CDISC03"],
                "DOMAIN": ["AE", "AE", "AE"],
                "USUBJID": ["TEST1", None, "ABC"],
                "values": ["TEST1", "TEST1", "TEST1"],
            },
            {"USUBJID": "%"},
            2,
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC02", "CDISC03"],
                "DOMAIN": ["AE", "AE", "AE"],
                "USUBJID": ["A1", "A2", "A"],
                "values": ["TEST1", "TEST1", "TEST1"],
            },
            {"USUBJID": "A%"},
            3,
        ),
    ],
)
def test_filtered_record_count(data, filter, expected):
    operation = setup_sql_operations("record_count", "values", data, extra_config={"filter": filter})
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, filter, grouping, expected",
    [
        (
            {
                "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE", "AE"],
                "EQ": [1, 2, 2],
                "values": ["TEST1", "TEST1", "TEST2"],
            },
            {},
            ["STUDYID"],
            [
                {"params": {"$1": "CDISC01"}, "value": [2]},
                {"params": {"$1": "CDISC02"}, "value": [1]},
            ],
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE", "AE"],
                "EQ": [1, 2, 2],
                "values": ["TEST1", "TEST1", "TEST2"],
            },
            {"EQ": 1},
            ["STUDYID"],
            [
                {"params": {"$1": "CDISC01"}, "value": [1]},
                {"params": {"$1": "CDISC02"}, "value": [0]},
            ],
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                "DOMAIN": ["AE", "AE", "AE"],
                "EQ": [2, 2, 2],
                "values": ["TEST1", "TEST1", "TEST2"],
            },
            {"EQ": 2},
            ["DOMAIN", "STUDYID"],
            [
                {"params": {"$1": "AE", "$2": "CDISC01"}, "value": [2]},
                {"params": {"$1": "AE", "$2": "CDISC02"}, "value": [1]},
            ],
        ),
        (
            {
                "STUDYID": ["CDISC01", "CDISC01", "CDISC01", "CDISC02", "CDISC02"],
                "DOMAIN": ["AE", None, None, None, None],
                "values": ["TEST1", "TEST2", "TEST1", "TEST1", "TEST1"],
                "AESEQ": [1, 1, 1, 1, 1],
            },
            {},
            ["STUDYID", "DOMAIN"],
            [
                {"params": {"$1": "CDISC01", "$2": "AE"}, "value": [1]},
                {"params": {"$1": "CDISC01", "$2": None}, "value": [2]},
                {"params": {"$1": "CDISC02", "$2": None}, "value": [2]},
            ],
        ),
        (
            {
                "STUDYID": ["CDISC01"] * 5,
                "DOMAIN": ["SUPPDM"] * 5,
                "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1", "SUBJ2", "SUBJ2"],
                "values": ["RACE1", "RACE2", "AGEU", "RACEOTH", "AGEU"],
            },
            {"values": "RACE%"},
            ["USUBJID"],
            [
                {"params": {"$1": "SUBJ1"}, "value": [2]},
                {"params": {"$1": "SUBJ2"}, "value": [1]},
            ],
        ),
    ],
)
def test_filtered_grouped_record_count(data, filter, grouping, expected):
    operation = setup_sql_operations(
        "record_count", "values", data, extra_config={"filter": filter, "grouping": grouping}
    )
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)


REGEX_DATA = {
    "STUDYID": ["CDISC01", "CDISC01", "CDISC01", "CDISC02", "CDISC02"],
    "DOMAIN": ["AE", "AE", "AE", "AE", "AE"],
    "values": ["2023-01-15T10:30", "2023-01-15", "2023-01", "O'BRIEN", None],
}


@pytest.mark.parametrize(
    "target, regex, filter, expected",
    [
        ("values", r"^\d{4}-\d{2}-\d{2}", None, 2),
        ("values", r"^\d{4}-\d{2}$", None, 1),
        ("values", r"T\d{2}:", None, 1),
        ("values", r"^O'B", None, 1),
        ("values", r"^NOMATCH", None, 0),
        ("values", r"^\d{4}", {"STUDYID": "CDISC02"}, 0),
        (None, r"^NOMATCH", None, 5),
    ],
)
def test_regex_record_count(target, regex, filter, expected):
    operation = setup_sql_operations(
        "record_count", target, REGEX_DATA, extra_config={"regex": regex, "filter": filter}
    )
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


def test_regex_grouped_record_count():
    operation = setup_sql_operations(
        "record_count", "values", REGEX_DATA, extra_config={"regex": r"^\d{4}-\d{2}", "grouping": ["STUDYID"]}
    )
    result = operation.execute()
    assert_operation_parameterized_constant(
        operation,
        result,
        [
            {"params": {"$1": "CDISC01"}, "value": [3]},
            {"params": {"$1": "CDISC02"}, "value": [0]},
        ],
    )


# TODO: Handle operation variables in other operations
"""
def test_operation_result_grouping_record_count(operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    data = PandasDataset.from_dict(
        {
            "STUDYID": ["STUDY1", "STUDY1", "STUDY1", "STUDY2", "STUDY2"],
            "DOMAIN": ["AE", "AE", "DM", "AE", "DM"],
            "USUBJID": ["SUBJ1", "SUBJ2", "SUBJ1", "SUBJ1", "SUBJ1"],
            "AESEQ": [1, 1, None, 1, None],
            "$group_cols": [
                ["STUDYID", "DOMAIN"],
                ["STUDYID", "DOMAIN"],
                ["STUDYID", "DOMAIN"],
                ["STUDYID", "DOMAIN"],
                ["STUDYID", "DOMAIN"],
            ],
        }
    )
    operation_params.dataframe = data
    operation_params.grouping = ["$group_cols"]
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert "STUDYID" in result
    assert "DOMAIN" in result
    operation_result = result[operation_params.operation_id]
    expected_series = pd.Series([2, 2, 1, 1, 1], name="operation_id", dtype="int64")
    assert operation_result.equals(expected_series)"""
