from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from .helpers import (
    assert_operation_collection,
)


def get_mock_library_metadata():
    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": {"AE"},
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {"name": "AETEST", "ordinal": 1, "core": "Req"},
                            {"name": "AENEW", "ordinal": 2, "core": "Exp"},
                            {"name": "AEPERM", "ordinal": 3, "core": "Perm"},
                        ],
                    }
                ],
            }
        ],
    }

    model_metadata = {
        "datasets": [{"name": "AE"}],
        "classes": [
            {
                "name": "Events",
                "classVariables": [
                    {"name": "--SEQ", "ordinal": 1, "core": "Req"},
                    {"name": "--TERM", "ordinal": 2},
                ],
            }
        ],
    }

    return LibraryMetadataContainer(standard_metadata=standard_metadata, model_metadata=model_metadata)


def test_sql_expected_variables():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="ae",
        column_data={"STUDYID": ["TEST1"], "AETERM": ["Headache"]},
    )

    library_metadata = get_mock_library_metadata()
    params = SqlOperationParams(domain="ae", target=None, standard="sdtmig", standard_version="3-4")

    operation = SqlOperationsFactory.get_service(
        "expected_variables", params, data_service, library_metadata=library_metadata
    )
    result = operation.execute()

    expected_vars = ["AENEW"]
    assert_operation_collection(operation, result, expected_vars, unsorted=True)


def test_sql_expected_variables_no_expected():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="dm",
        column_data={"STUDYID": ["TEST1"]},
    )

    standard_metadata = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": {"DM"},
        "classes": [
            {
                "name": "Special Purpose",
                "datasets": [
                    {
                        "name": "DM",
                        "datasetVariables": [
                            {"name": "RFSTDTC", "ordinal": 1, "core": "Req"},
                        ],
                    }
                ],
            }
        ],
    }
    model_metadata = {"datasets": [], "classes": []}
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata, model_metadata=model_metadata)

    params = SqlOperationParams(domain="dm", target=None, standard="sdtmig", standard_version="3-4")

    operation = SqlOperationsFactory.get_service(
        "expected_variables", params, data_service, library_metadata=library_metadata
    )
    result = operation.execute()

    assert result.query == "SELECT NULL AS value WHERE FALSE"
