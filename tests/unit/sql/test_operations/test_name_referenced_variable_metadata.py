from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory


class DummyStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {
            "classes": [
                {
                    "datasets": [
                        {
                            "datasetVariables": [
                                {"name": "EPOCH", "role": "Timing", "core": "Perm"},
                                {"name": "AESEV", "role": "Record Qualifier", "core": "Perm"},
                            ]
                        }
                    ]
                }
            ]
        }


def _lookup(attribute_name, key):
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(
        domain="AE", target="DUMMYVAR", attribute_name=attribute_name, standards_context=DummyStandardsContext()
    )
    result = SqlOperationsFactory.get_service("name_referenced_variable_metadata", params, data_service).execute()
    assert (result.type, result.subtype, result.params) == ("collection", "Char", {"%target%": "DUMMYVAR"})
    data_service.pgi.execute_sql(result.query.replace("%target%", f"'{key}'"))
    return [row["value"] for row in data_service.pgi.fetch_all()]


def test_name_referenced_variable_metadata_role():
    assert _lookup("role", "EPOCH") == ["Timing"]
    assert _lookup("role", "AESEV") == ["Record Qualifier"]
    assert _lookup("role", "DUMMYNAME") == []


def test_name_referenced_variable_metadata_core():
    assert _lookup("core", "EPOCH") == ["Perm"]
    assert _lookup("core", "DUMMYNAME") == []
