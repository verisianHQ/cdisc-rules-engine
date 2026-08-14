import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

import cdisc_rules_engine.data_service.startup.populate_codelists as populate_codelists_module
import cdisc_rules_engine.data_service.startup.populate_dictionaries as populate_dictionaries_module
import cdisc_rules_engine.data_service.startup.populate_helper_tables as populate_helper_tables_module
import cdisc_rules_engine.data_service.startup.populate_standards as populate_standards_module
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes


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


def test_populate_codelists_reports_progress(tmp_path, monkeypatch):
    pgi = MagicMock()
    reporter = _RecordingReporter()

    fake_root = tmp_path / "root"
    cache_dir = fake_root / "cache"
    cache_dir.mkdir(parents=True)
    ct_file = cache_dir / "ct-test.json"
    ct_file.write_text("{}")

    monkeypatch.setattr(populate_codelists_module, "ROOT_PATH", fake_root)

    class _FakeCodelistReader:
        def __init__(self, file_path):
            self.file_path = file_path

        def read(self):
            return [{"name": "A"}, {"name": "B"}]

    monkeypatch.setattr(populate_codelists_module, "CodelistReader", _FakeCodelistReader)

    populate_codelists_module.populate_codelists(
        pgi,
        cache_path="cache",
        codelists=["ct-test.json"],
        progress_reporter=reporter,
    )

    assert reporter.calls[0][0] == "start"
    assert any(c[0] == "start_file" and c[3] == "ct-test.json" and c[5] == 2 for c in reporter.calls)
    assert ("report_chunk", 2) in reporter.calls
    assert ("end_file", 2) in reporter.calls
    assert reporter.calls[-1] == ("finish",)


def test_populate_standards_reports_progress(tmp_path, monkeypatch):
    pgi = MagicMock()
    reporter = _RecordingReporter()

    standards_file = tmp_path / "sdtm.json"
    standards_file.write_text("{}")

    class _FakeStandardsReader:
        def __init__(self, file_path):
            self.file_path = file_path

        def read(self):
            return {
                "datasets": [{"dataset_name": "AE"}],
                "variables": [{"variable_name": "USUBJID"}, {"variable_name": "STUDYID"}],
            }

    monkeypatch.setattr(populate_standards_module, "MetadataStandardsReader", _FakeStandardsReader)

    populate_standards_module.populate_standards(
        pgi,
        path=tmp_path,
        progress_reporter=reporter,
    )

    assert reporter.calls[0][0] == "start"
    assert any(c[0] == "start_file" and c[3] == "sdtm.json" and c[5] == 3 for c in reporter.calls)
    assert ("report_chunk", 1) in reporter.calls
    assert ("report_chunk", 2) in reporter.calls
    assert ("end_file", 3) in reporter.calls
    assert reporter.calls[-1] == ("finish",)


def test_populate_helper_tables_reports_progress(tmp_path, monkeypatch):
    pgi = MagicMock()
    reporter = _RecordingReporter()

    helper_file = tmp_path / "helper.json"
    helper_file.write_text(json.dumps([{"fda_guides": "X"}, {"fda_guides": "Y"}]))

    monkeypatch.setattr(populate_helper_tables_module, "HELPER_DATA_PATH", tmp_path)
    monkeypatch.setattr(
        populate_helper_tables_module,
        "SCHEMA_MAP",
        {Path("helper.json"): populate_helper_tables_module._cg_taugs_schema},
    )

    populate_helper_tables_module.populate_helper_tables(
        pgi,
        progress_reporter=reporter,
    )

    assert reporter.calls[0][0] == "start"
    assert any(c[0] == "start_file" and c[3] == "helper.json" and c[5] == 2 for c in reporter.calls)
    assert ("report_chunk", 2) in reporter.calls
    assert ("end_file", 2) in reporter.calls
    assert reporter.calls[-1] == ("finish",)


def test_populate_dictionaries_reports_progress(tmp_path):
    pgi = MagicMock()
    reporter = _RecordingReporter()

    dictionary_file = tmp_path / "meddra.csv"
    dictionary_file.write_text("dummy")

    class _FakeDictionaryReader:
        def __init__(self, pgi_obj, path):
            self.path = path

        def _extract_version_metadata(self):
            return None

        def process_data(self, metadata):
            return pd.DataFrame(
                [
                    {"term_code": "1", "term_name": "A", "term_type": "LLT"},
                    {"term_code": "2", "term_name": "B", "term_type": "LLT"},
                ]
            )

    class _FakeExternalDictionaries:
        def get_all_implemented_reader_classes(self):
            return {DictionaryTypes.MEDDRA.value: _FakeDictionaryReader}

        def get_dictionary_path(self, dictionary_type):
            return str(dictionary_file)

    populate_dictionaries_module.populate_dictionaries(
        pgi,
        external_dictionaries=_FakeExternalDictionaries(),
        progress_reporter=reporter,
    )

    assert reporter.calls[0][0] == "start"
    assert any(c[0] == "start_file" and c[3] == DictionaryTypes.MEDDRA.value and c[5] == 2 for c in reporter.calls)
    assert ("report_chunk", 2) in reporter.calls
    assert ("end_file", 2) in reporter.calls
    assert reporter.calls[-1] == ("finish",)
