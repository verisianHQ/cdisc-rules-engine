import os
from types import SimpleNamespace
from unittest.mock import MagicMock

from cdisc_rules_engine.sql_dataset_builders.sql_stf_dataset_builder import SqlSTFDatasetBuilder


def test_sql_stf_dataset_builder_creates_rows_with_file_tags_and_operations():
    stf_path = (
        f"{os.path.dirname(__file__)}/../../../resources/stf/"
        "STFV2-6-1/Sample/0004/m53/study-ich-12345/stf/v2-stf1.xml"
    )

    data_service = MagicMock()
    data_service.pgi.schema.get_table.return_value = None

    def _read_file(dataset_name):
        with open(dataset_name, "rb") as file:
            return file.read()

    data_service.get_define_xml_contents.side_effect = _read_file

    dataset_metadata = SimpleNamespace(name="stf_test", filepath=stf_path)

    builder = SqlSTFDatasetBuilder(
        rule={},
        data_service=data_service,
        dataset_metadata=dataset_metadata,
        standards_context=MagicMock(),
        stf_file_path=stf_path,
    )

    table_name = builder.build()

    assert table_name == "stf_test_stf_metadata"
    data_service.pgi.create_table.assert_called_once()
    data_service.pgi.insert_data.assert_called_once()

    insert_table_name, inserted_rows = data_service.pgi.insert_data.call_args.args
    assert insert_table_name == "stf_test_stf_metadata"
    assert len(inserted_rows) == 2
    assert all(row["file_tag_name"] == "synopsis" for row in inserted_rows)
    assert all(row["document_operation"] == "new" for row in inserted_rows)
