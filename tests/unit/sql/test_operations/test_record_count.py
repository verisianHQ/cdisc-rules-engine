import pytest

from .helpers import (
    assert_operation_constant,
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
        # (
        #     {
        #         "STUDYID": ["CDISC01", "CDISC02", "CDISC03"],
        #         "DOMAIN": ["AE", "AE", "DM"],
        #         "EQ": [1, 2, 3],
        #         "USUBJID": ["TEST1", "TEST2", "ABC"],
        #     },
        #     {"USUBJID": "TEST%"},
        #     2,
        # ),
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
            [2, 2, 1],
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
            [1, 1, 0],
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
            [2, 2, 1],
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
            [1, 2, 2, 2, 2],
        ),
    ],
)
def test_filtered_grouped_record_count(data, filter, grouping, expected):
    operation = setup_sql_operations(
        "record_count", "values", data, extra_config={"filter": filter, "grouping": grouping}
    )
    result = operation.execute()

    assert result.type == "window"
    operation.data_service.pgi.execute_sql(result.query)
    query_results = operation.data_service.pgi.fetch_all()
    query_results.sort(key=lambda x: x["id"])
    actual_values = [row["value"] for row in query_results]

    assert actual_values == expected


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
