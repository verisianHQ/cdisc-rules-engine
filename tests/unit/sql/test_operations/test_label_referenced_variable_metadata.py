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
                                {"label": "Severity/Intensity", "name": "AESEV", "role": "Record Qualifier"},
                                {"label": "Epoch", "name": "EPOCH", "role": "Timing"},
                                {"label": "Subject's Reported Term", "name": "AETERM", "role": "Topic"},
                            ]
                        },
                        {
                            "datasetVariables": [
                                {"label": "Severity/Intensity", "name": "CESEV", "role": "Record Qualifier"},
                            ]
                        },
                    ]
                }
            ]
        }


class EmptyStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {"classes": []}


def _lookup(attribute_name, key, standards_context=None):
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(
        domain="AE",
        target="DUMMYVAR",
        attribute_name=attribute_name,
        standards_context=standards_context or DummyStandardsContext(),
    )
    result = SqlOperationsFactory.get_service("label_referenced_variable_metadata", params, data_service).execute()
    if result.type == "constant":
        return result
    assert (result.subtype, result.params) == ("Char", {"%target%": "DUMMYVAR"})
    literal = "NULL" if key is None else "'" + key.replace("'", "''") + "'"
    data_service.pgi.execute_sql(result.query.replace("%target%", literal))
    return [row["value"] for row in data_service.pgi.fetch_all()]


def test_label_referenced_variable_metadata_role():
    assert _lookup("role", "Epoch") == ["Timing"]
    assert _lookup("role", "DummyLabel") == []
    assert _lookup("role", None) == []


def test_label_referenced_variable_metadata_dedups_values_but_keeps_all_names():
    assert _lookup("role", "Severity/Intensity") == ["Record Qualifier"]
    assert _lookup("name", "Severity/Intensity") == ["AESEV", "CESEV"]


def test_label_referenced_variable_metadata_escapes_quotes():
    assert _lookup("role", "Subject's Reported Term") == ["Topic"]


def test_label_referenced_variable_metadata_empty_library():
    result = _lookup("role", "Epoch", EmptyStandardsContext())
    assert (result.type, result.query) == ("constant", "SELECT NULL")
