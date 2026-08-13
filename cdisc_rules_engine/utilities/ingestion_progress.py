import os
import sys
from abc import ABC, abstractmethod
from typing import List, Optional

import click

from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions


class IngestionProgressReporter(ABC):
    """
    Abstract callback interface for reporting SQL data ingestion progress.

    Implementations receive lifecycle events for the overall load operation
    (start/finish) and for each individual file (start_file, chunk, end_file).
    """

    @abstractmethod
    def start(self, total_files: int, total_bytes: int):
        """Called once before any files are loaded."""
        pass

    @abstractmethod
    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        """Called when a new dataset file starts loading."""
        pass

    @abstractmethod
    def report_chunk(self, rows: int):
        """Called after each chunk is inserted."""
        pass

    @abstractmethod
    def end_file(self, rows: int):
        """Called when a dataset file finishes loading."""
        pass

    @abstractmethod
    def finish(self):
        """Called once after all files are loaded."""
        pass


class DisabledIngestionProgressReporter(IngestionProgressReporter):
    """No-op progress reporter."""

    def start(self, total_files: int, total_bytes: int):
        pass

    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        pass

    def report_chunk(self, rows: int):
        pass

    def end_file(self, rows: int):
        pass

    def finish(self):
        pass


class PercentIngestionProgressReporter(IngestionProgressReporter):
    """
    Prints overall ingestion progress as integer percentages, one per file.
    """

    def __init__(self, output_stream=None):
        self.output_stream = output_stream or sys.stdout
        self._total_files = 0
        self._current_file = 0

    def start(self, total_files: int, total_bytes: int):
        self._total_files = total_files
        self._current_file = 0

    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        pass

    def report_chunk(self, rows: int):
        pass

    def end_file(self, rows: int):
        self._current_file += 1
        percent = int(self._current_file / self._total_files * 100)
        self.output_stream.write(f"{percent}\n")
        self.output_stream.flush()

    def finish(self):
        pass


class ClickIngestionProgressBar(IngestionProgressReporter):
    """
    Renders a click progress bar for ingestion.

    The bar tracks rows loaded across all files. When the total row count for a
    file is unknown, the bar advances by the number of rows loaded so far and
    may exceed the initial length estimate.
    """

    def __init__(self, label: str = "Loading datasets", output=None):
        self._label = label
        self._output = output or sys.stdout
        self._bar = None
        self._total_rows_estimate = 0
        self._rows_loaded = 0
        self._known_rows_total = False

    def start(self, total_files: int, total_bytes: int):
        pass

    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        if self._bar is not None:
            self._bar.finish()

        if total_rows is not None:
            self._total_rows_estimate += total_rows
            self._known_rows_total = True
            label = f"Loading {file_name} ({file_index + 1}/{total_files})"
        else:
            # If we don't know the total, use file bytes as a rough estimate.
            self._total_rows_estimate += max(file_bytes // 200, 1)
            self._known_rows_total = False
            label = f"Loading {file_name} ({file_index + 1}/{total_files})"

        self._bar = click.progressbar(
            length=self._total_rows_estimate,
            label=label,
            fill_char=click.style("\u2588", fg="green"),
            empty_char=click.style("-", fg="white", dim=True),
            show_eta=False,
            file=self._output,
        )
        self._bar.__enter__()
        # Recreate the bar at the current loaded position so previous files
        # keep their progress.
        self._bar.update(self._rows_loaded)

    def report_chunk(self, rows: int):
        self._rows_loaded += rows
        if self._bar is not None:
            # If we underestimated, extend the bar to keep it moving.
            if self._rows_loaded > self._bar.length:
                self._bar.length = self._rows_loaded
            self._bar.update(rows)

    def end_file(self, rows: int):
        if self._bar is not None:
            self._bar.__exit__(None, None, None)
            self._bar = None

    def finish(self):
        if self._bar is not None:
            self._bar.__exit__(None, None, None)
            self._bar = None


def get_ingestion_progress_reporter(progress_option: str) -> IngestionProgressReporter:
    """
    Return an ingestion progress reporter matching the CLI progress option.
    """
    if progress_option == ProgressParameterOptions.DISABLED.value:
        return DisabledIngestionProgressReporter()
    if progress_option == ProgressParameterOptions.PERCENTS.value:
        return PercentIngestionProgressReporter()
    if progress_option == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        return DisabledIngestionProgressReporter()
    return ClickIngestionProgressBar()


def get_total_dataset_size(dataset_paths: List[str]) -> int:
    """Sum the byte sizes of all dataset files."""
    return sum(os.path.getsize(path) for path in dataset_paths if os.path.exists(path))
