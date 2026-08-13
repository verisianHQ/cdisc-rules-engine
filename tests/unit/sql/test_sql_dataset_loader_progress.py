from unittest.mock import MagicMock, patch

import pytest

from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2, VariableMetadata


class _FakeReader:
    def __init__(self, file_path: str, chunks, total_rows):
        self.file_path = file_path
        self._chunks = chunks
        self._total_rows = total_rows

    def read(self):
        metadata = DatasetMetadata2(
            filename="ae.xpt",
            name="ae",
            label="Adverse Events",
            variables=[
                VariableMetadata(
                    name="USUBJID",
                    label="Subject Identifier",
                    type="Char",
                    length=20,
                    format="",
                    order=1,
                )
            ],
        )
        return metadata, iter(self._chunks)

    def _get_total_rows(self):
        return self._total_rows


class _RecordingReporter:
    def __init__(self):
        self.calls = []

    def start(self, total_files: int, total_bytes: int):
        self.calls.append(("start", total_files, total_bytes))

    def start_file(self, file_index, total_files, file_name, file_bytes, total_rows):
        self.calls.append(("start_file", file_index, total_files, file_name, file_bytes, total_rows))

    def report_chunk(self, rows: int):
        self.calls.append(("report_chunk", rows))

    def end_file(self, rows: int):
        self.calls.append(("end_file", rows))

    def finish(self):
        self.calls.append(("finish",))


@pytest.fixture
def fake_pgi():
    pgi = MagicMock()
    pgi.sql_namespace = None
    return pgi


def test_load_datasets_reports_progress_per_chunk(fake_pgi, tmp_path):
    """SqlDatasetLoader should invoke reporter callbacks through the file/chunk lifecycle."""
    file_path = tmp_path / "ae.xpt"
    file_path.write_text("dummy")

    chunks = [
        [{"usubjid": "001"}, {"usubjid": "002"}],
        [{"usubjid": "003"}],
    ]
    fake_reader = _FakeReader(str(file_path), chunks, total_rows=3)

    with patch(
        "cdisc_rules_engine.data_service.loading.load_datasets.DataReaderFactory.get_data_reader",
        return_value=fake_reader,
    ):
        reporter = _RecordingReporter()
        results = SqlDatasetLoader.load_datasets(fake_pgi, [str(file_path)], progress_reporter=reporter)

    assert len(results) == 1
    assert results[0].name == "ae"

    # Reporter lifecycle
    assert reporter.calls[0][0] == "start"
    assert reporter.calls[0][1] == 1  # total_files
    start_file_call = next(c for c in reporter.calls if c[0] == "start_file")
    assert start_file_call[1] == 0  # file_index
    assert start_file_call[3] == "ae.xpt"
    assert start_file_call[5] == 3  # total_rows

    chunk_calls = [c for c in reporter.calls if c[0] == "report_chunk"]
    assert chunk_calls == [("report_chunk", 2), ("report_chunk", 1)]

    end_file_call = next(c for c in reporter.calls if c[0] == "end_file")
    assert end_file_call[1] == 3

    assert reporter.calls[-1] == ("finish",)

    # Data should be passed through with lowercased column names.
    assert fake_pgi.create_table.called
    assert fake_pgi.insert_data.call_count == 2
    first_insert = fake_pgi.insert_data.call_args_list[0][0][1]
    assert first_insert == [{"usubjid": "001"}, {"usubjid": "002"}]

    # Finalisation should run after all chunks.
    assert fake_pgi.execute_sql.called


def test_load_datasets_without_reporter_uses_disabled(fake_pgi, tmp_path):
    """SqlDatasetLoader should default to a no-op reporter when none is supplied."""
    file_path = tmp_path / "dm.xpt"
    file_path.write_text("dummy")

    fake_reader = _FakeReader(str(file_path), [[{"usubjid": "001"}]], total_rows=1)

    with patch(
        "cdisc_rules_engine.data_service.loading.load_datasets.DataReaderFactory.get_data_reader",
        return_value=fake_reader,
    ):
        results = SqlDatasetLoader.load_datasets(fake_pgi, [str(file_path)])

    assert len(results) == 1
    assert fake_pgi.insert_data.called
