from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory

TEST_DATASET = {
    "DUMMYVAR": ["Severity/Intensity", "Epoch", "DummyLabel"],
}


class DummyStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {
            "classes": [
                {
                    "datasets": [
                        {
                            "datasetVariables": [
                                {"label": "Epoch", "role": "Timing", "core": "Perm"},
                                {"label": "Severity/Intensity", "role": "Record Qualifier", "core": "Perm"},
                            ]
                        }
                    ]
                }
            ]
        }


def test_label_referenced_variable_metadata_role():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service=data_service, table_name="AE", column_data=TEST_DATASET, standards_context=DummyStandardsContext()
    )

    params = SqlOperationParams(
        domain="AE", target="DUMMYVAR", attribute_name="role", standards_context=DummyStandardsContext()
    )
    operation = SqlOperationsFactory.get_service("label_referenced_variable_metadata", params, data_service)

    result = operation.execute()

    assert result.type == "collection"
    assert result.subtype == "Char"
    assert result.params == {"%target%": "DUMMYVAR"}

    assert result.query.startswith("SELECT value FROM (VALUES ")
    assert "('Epoch', 'Timing')" in result.query
    assert "('Severity/Intensity', 'Record Qualifier')" in result.query
    assert "DummyLabel" not in result.query
    assert result.query.endswith(" AS t(lbl, value) WHERE t.lbl = %target%")


def test_label_referenced_variable_metadata_core():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service=data_service, table_name="AE", column_data=TEST_DATASET, standards_context=DummyStandardsContext()
    )

    params = SqlOperationParams(
        domain="AE", target="DUMMYVAR", attribute_name="core", standards_context=DummyStandardsContext()
    )
    operation = SqlOperationsFactory.get_service("label_referenced_variable_metadata", params, data_service)

    result = operation.execute()

    assert result.type == "collection"
    assert result.subtype == "Char"
    assert result.params == {"%target%": "DUMMYVAR"}

    assert result.query.startswith("SELECT value FROM (VALUES ")
    assert "('Epoch', 'Perm')" in result.query
    assert "('Severity/Intensity', 'Perm')" in result.query
    assert "DummyLabel" not in result.query
    assert result.query.endswith(" AS t(lbl, value) WHERE t.lbl = %target%")


class WideStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {
            "classes": [
                {
                    "datasets": [
                        {
                            "datasetVariables": [
                                {"label": "Severity/Intensity", "role": "Record Qualifier"},
                                {"label": "Epoch", "role": "Timing"},
                                {"label": "Study Identifier", "role": "Identifier"},
                            ]
                        },
                        {
                            "datasetVariables": [
                                {"label": "Severity/Intensity", "role": "Record Qualifier"},
                                {"label": "Subject's Reported Term", "role": "Topic"},
                            ]
                        },
                    ]
                }
            ]
        }


def _run_label_role(column_data, standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service=data_service, table_name="AE", column_data=column_data, standards_context=standards_context
    )
    params = SqlOperationParams(
        domain="AE", target="DUMMYVAR", attribute_name="role", standards_context=standards_context
    )
    return SqlOperationsFactory.get_service("label_referenced_variable_metadata", params, data_service).execute()


def test_label_referenced_variable_metadata_only_keeps_labels_in_data_once():
    result = _run_label_role(
        {"DUMMYVAR": ["Severity/Intensity", "Severity/Intensity", "Epoch", "DummyLabel"]}, WideStandardsContext()
    )

    assert result.query.count("('Severity/Intensity', 'Record Qualifier')") == 1
    assert "('Epoch', 'Timing')" in result.query
    assert "Study Identifier" not in result.query
    assert "Reported Term" not in result.query
    assert result.params == {"%target%": "DUMMYVAR"}


def test_label_referenced_variable_metadata_escapes_quotes():
    result = _run_label_role({"DUMMYVAR": ["Subject's Reported Term"]}, WideStandardsContext())

    assert "('Subject''s Reported Term', 'Topic')" in result.query


def test_label_referenced_variable_metadata_no_labels_in_data():
    result = _run_label_role({"DUMMYVAR": ["DummyLabel", "Other"]}, WideStandardsContext())

    assert result.type == "collection"
    assert result.params == {"%target%": "DUMMYVAR"}
    assert result.query == (
        "SELECT value FROM (VALUES (NULL::text, NULL::text)) AS t(lbl, value) WHERE t.lbl = %target%"
    )
