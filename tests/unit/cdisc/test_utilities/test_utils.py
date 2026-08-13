import pytest
from unittest.mock import patch
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.utilities.utils import (
    get_corresponding_datasets,
    get_execution_status,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

mock_datasets = [
    {"filename": "SS11.xpt", "first_record": {"DOMAIN": "SS"}},
]


@patch(
    "cdisc_rules_engine.utilities.utils.get_corresponding_datasets",
    return_value=mock_datasets,
)
def test_is_split_dataset_from_file(mock_get_corresponding_datasets):
    result = (SDTMDatasetMetadata(filename="SS11.xpt", first_record={"DOMAIN": "SS"}).is_split,)
    assert result


datasets_tests = [
    (
        {"name": "SS", "first_record": {"RDOMAIN": "SS"}},
        False,
    ),
    (
        {"name": "SUPPSS", "first_record": {"RDOMAIN": "SS"}},
        True,
    ),
    ({"name": "SUPPSS1", "first_record": {"RDOMAIN": "SS"}}, True),
    ({"name": "SQAPSSS1", "first_record": {"RDOMAIN": "APSS"}}, True),
]


@pytest.mark.parametrize("mock_dataset, expected", datasets_tests)
def test_is_supp_dataset(mock_dataset, expected):
    result = SDTMDatasetMetadata(**mock_dataset).is_supp
    assert result == expected, f"Expected {expected} but got {result} for datasets {mock_datasets}"


datasets = [
    SDTMDatasetMetadata(**dataset)
    for dataset in [
        {"filename": "SS.xpt", "first_record": {"DOMAIN": "SS"}},
        {"filename": "SS12.xpt", "first_record": {"DOMAIN": "SS"}},
        {"filename": "AE.xpt", "first_record": {"DOMAIN": "AE"}},
        {"filename": "DD.xpt", "first_record": {"DOMAIN": "DD"}},
        {"filename": "EC.xpt", "first_record": {"DOMAIN": "EC"}},
        {"filename": "EX.xpt", "first_record": {"DOMAIN": "EX"}},
        {"filename": "FA.xpt", "first_record": {"DOMAIN": "FA"}},
        {"filename": "FT.xpt", "first_record": {"DOMAIN": "FT"}},
        {"filename": "RS.xpt", "first_record": {"DOMAIN": "RS"}},
        {"filename": "AB.xpt", "first_record": {"DOMAIN": "AB"}},
        {"filename": "AB12.xpt", "first_record": {"DOMAIN": "AB"}},
    ]
]


# Parameters for testing each domain
domain_test_cases = [
    (
        "SS",
        [
            {"filename": "SS.xpt", "first_record": {"DOMAIN": "SS"}},
            {"filename": "SS12.xpt", "first_record": {"DOMAIN": "SS"}},
        ],
    ),
    (
        "AB",
        [
            {"filename": "AB.xpt", "first_record": {"DOMAIN": "AB"}},
            {"filename": "AB12.xpt", "first_record": {"DOMAIN": "AB"}},
        ],
    ),
    ("AE", [{"filename": "AE.xpt", "first_record": {"DOMAIN": "AE"}}]),
    ("DD", [{"filename": "DD.xpt", "first_record": {"DOMAIN": "DD"}}]),
    ("EC", [{"filename": "EC.xpt", "first_record": {"DOMAIN": "EC"}}]),
    ("EX", [{"filename": "EX.xpt", "first_record": {"DOMAIN": "EX"}}]),
    ("FA", [{"filename": "FA.xpt", "first_record": {"DOMAIN": "FA"}}]),
    ("FT", [{"filename": "FT.xpt", "first_record": {"DOMAIN": "FT"}}]),
    ("RS", [{"filename": "RS.xpt", "first_record": {"DOMAIN": "RS"}}]),
]


@pytest.mark.parametrize("domain, expected_datasets", domain_test_cases)
def test_get_corresponding_datasets(domain, expected_datasets):
    result_datasets = get_corresponding_datasets(datasets, SDTMDatasetMetadata(first_record={"DOMAIN": domain}))
    assert result_datasets == [
        SDTMDatasetMetadata(**dataset) for dataset in expected_datasets
    ], f"The function should return only datasets matching the '{domain}' domain"


def test_get_execution_status_empty_results():
    assert get_execution_status([]) == ExecutionStatus.SUCCESS.value


def test_get_execution_status_all_success():
    results = [
        {"executionStatus": ExecutionStatus.SUCCESS.value},
        {"executionStatus": ExecutionStatus.SUCCESS.value},
    ]
    assert get_execution_status(results) == ExecutionStatus.SUCCESS.value


def test_get_execution_status_all_skipped():
    results = [
        {"executionStatus": ExecutionStatus.SKIPPED.value},
        {"executionStatus": ExecutionStatus.SKIPPED.value},
    ]
    assert get_execution_status(results) == ExecutionStatus.SKIPPED.value


def test_get_execution_status_all_resource_limit():
    results = [
        {"executionStatus": ExecutionStatus.RESOURCE_LIMIT.value},
        {"executionStatus": ExecutionStatus.RESOURCE_LIMIT.value},
    ]
    assert get_execution_status(results) == ExecutionStatus.RESOURCE_LIMIT.value


def test_get_execution_status_partial_success():
    results = [
        {"executionStatus": ExecutionStatus.SUCCESS.value},
        {"executionStatus": ExecutionStatus.RESOURCE_LIMIT.value},
    ]
    assert get_execution_status(results) == ExecutionStatus.PARTIAL_SUCCESS.value
