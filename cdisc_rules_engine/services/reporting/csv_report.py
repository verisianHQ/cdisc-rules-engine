import csv
import os
from typing import BinaryIO, Iterable, List, Optional

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from .base_report import BaseReport

CSV_HEADER = ["Rule ID", "Dataset", "Record", "Variable", "Value", "Message"]


class CsvReport(BaseReport):
    """
    Writes a results.csv with one row per variable of each issue (one row if it has no variables).
    Execution errors get a row with "EXECUTION_ERROR" as the variable.
    """

    def __init__(
        self,
        datasets: Iterable[dict],
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(datasets, dataset_paths, validation_results, elapsed_time, args, template)
        self._item_type = "dict"

    @property
    def _file_format(self) -> str:
        return ReportTypes.CSV.value.lower()

    def get_export(
        self, define_version=None, cdiscCt=None, standard=None, version=None, dictionary_versions={}, **kwargs
    ):
        rows = []
        for issue in self.get_detailed_data():
            dataset = self._display_dataset_name(issue.get("dataset")) or ""
            pairs = list(zip(issue.get("variables") or [], issue.get("values") or [])) or [("", "")]
            for variable, value in pairs:
                rows.append(
                    [
                        issue.get("core_id") or "",
                        dataset,
                        str(issue.get("row", "")),
                        variable,
                        "" if value in (None, "null") else value,
                        issue.get("message") or "",
                    ]
                )
        for validation_result in self._results:
            for result in validation_result.results or []:
                if result.get("executionStatus") != ExecutionStatus.EXECUTION_ERROR.value:
                    continue
                message = result.get("message") or ""
                for error in result.get("errors") or [{}]:
                    dataset = self._display_dataset_name(error.get("dataset") or result.get("dataset")) or ""
                    rows.append(
                        [
                            validation_result.id or "",
                            dataset,
                            "",
                            "EXECUTION_ERROR",
                            error.get("message") or message,
                            message,
                        ]
                    )
        return CSV_HEADER, sorted(rows, key=_csv_sort_key)

    def write_report(self, **kwargs):
        output_dir = os.path.dirname(self._output_name)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        header, rows = self.get_export()
        with open(self._output_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        if self._template:
            self._template.close()


def _csv_sort_key(row: List[str]) -> tuple:
    parts = []
    for field in row:
        try:
            parts.append((0, int(field), ""))
        except ValueError:
            parts.append((1, 0, field))
    return tuple(parts)
