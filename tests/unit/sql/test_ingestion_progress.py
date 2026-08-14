from io import StringIO

import pytest

from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.utilities.ingestion_progress import (
    ClickIngestionProgressBar,
    DisabledIngestionProgressReporter,
    PercentIngestionProgressReporter,
    get_ingestion_progress_reporter,
)


class _FakeTerminal(StringIO):
    """A StringIO that claims to be a TTY so click renders the bar."""

    def isatty(self):
        return True


class _RecordingReporter(DisabledIngestionProgressReporter):
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


def test_disabled_reporter_does_not_raise():
    reporter = DisabledIngestionProgressReporter()
    reporter.start(2, 100)
    reporter.start_file(0, 2, "ae.xpt", 50, 10)
    reporter.report_chunk(5)
    reporter.end_file(10)
    reporter.finish()


def test_percent_reporter_emits_per_file_percentages():
    output = StringIO()
    reporter = PercentIngestionProgressReporter(output_stream=output)
    reporter.start(total_files=2, total_bytes=100)
    reporter.start_file(0, 2, "ae.xpt", 50, total_rows=10)
    reporter.report_chunk(10)
    reporter.end_file(10)
    reporter.start_file(1, 2, "dm.xpt", 50, total_rows=10)
    reporter.report_chunk(10)
    reporter.end_file(10)
    reporter.finish()

    lines = output.getvalue().splitlines()
    assert lines == ["50", "100"]


def test_click_progress_bar_moves_with_known_row_totals():
    output = _FakeTerminal()
    reporter = ClickIngestionProgressBar(label="Loading datasets", output=output)
    reporter.start(total_files=1, total_bytes=100)
    reporter.start_file(0, 1, "ae.xpt", 100, total_rows=5)
    reporter.report_chunk(2)
    reporter.report_chunk(3)
    reporter.end_file(5)
    reporter.finish()

    rendered = output.getvalue()
    assert "Loading ae.xpt" in rendered
    assert "#" in rendered


def test_click_progress_bar_extends_when_total_unknown():
    output = _FakeTerminal()
    reporter = ClickIngestionProgressBar(label="Loading datasets", output=output)
    reporter.start(total_files=1, total_bytes=1000)
    reporter.start_file(0, 1, "ae.sas7bdat", 1000, total_rows=None)
    reporter.report_chunk(100)
    reporter.end_file(100)
    reporter.finish()

    rendered = output.getvalue()
    assert "Loading ae.sas7bdat" in rendered
    assert "#" in rendered


@pytest.mark.parametrize(
    "option,expected_class",
    [
        (ProgressParameterOptions.DISABLED.value, DisabledIngestionProgressReporter),
        (ProgressParameterOptions.PERCENTS.value, PercentIngestionProgressReporter),
        (ProgressParameterOptions.VERBOSE_OUTPUT.value, DisabledIngestionProgressReporter),
        (ProgressParameterOptions.BAR.value, ClickIngestionProgressBar),
        ("unknown_option", ClickIngestionProgressBar),
    ],
)
def test_get_ingestion_progress_reporter_mapping(option, expected_class):
    reporter = get_ingestion_progress_reporter(option)
    assert isinstance(reporter, expected_class)


def test_recording_reporter_captures_lifecycle():
    reporter = _RecordingReporter()
    reporter.start(1, 200)
    reporter.start_file(0, 1, "ae.xpt", 200, 10)
    reporter.report_chunk(10)
    reporter.end_file(10)
    reporter.finish()

    assert reporter.calls[0] == ("start", 1, 200)
    assert reporter.calls[1][:4] == ("start_file", 0, 1, "ae.xpt")
    assert reporter.calls[2] == ("report_chunk", 10)
    assert reporter.calls[3] == ("end_file", 10)
    assert reporter.calls[4] == ("finish",)
