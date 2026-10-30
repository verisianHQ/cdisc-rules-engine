import re
from abc import ABC, abstractmethod
from io import IOBase
from pathlib import Path
from typing import List, Optional, Union, Iterable

from openpyxl import Workbook

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

_PER_DATASET_SKIP_REASON = re.compile(r"^(?P<reason>.+?) for rule id=[^,]*, dataset=(?P<dataset>\S+)$")


class BaseReport(ABC):
    """
    Generates a base report for a given set of validation results.
    """

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[IOBase] = None,
    ):
        self._datasets = datasets
        self._dataset_paths: Iterable[str] = dataset_paths
        self._elapsed_time: float = elapsed_time
        self._results: List[RuleValidationResult] = validation_results
        self._item_type = ""
        self._args = args
        self._template = template
        self._output_name: str = f"{self._args.output}.{self._file_format}"

    def get_summary_data(self) -> List[List]:
        """
        Generates the Issue Summary data that goes into the export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "Dataset",
            "CORE-ID",
            "Message",
            "Issues",
            "Explanation"
        ]
        """
        summary_data = []
        for validation_result in self._results:
            if validation_result.execution_status == "success":
                for result in validation_result.results or []:
                    dataset = result.get("dataset")
                    if result.get("errors") and result.get("executionStatus") == "success":
                        summary_item = {
                            "dataset": dataset,
                            "core_id": validation_result.id,
                            "message": result.get("message"),
                            "issues": len(result.get("errors")),
                        }

                        if self._item_type == "list":
                            summary_data.extend([[*summary_item.values()]])
                        elif self._item_type == "dict":
                            summary_data.extend([summary_item])

        return sorted(
            summary_data,
            key=lambda x: ((x[0], x[1]) if (self._item_type == "list") else (x["dataset"], x["core_id"])),
        )

    def get_detailed_data(self, excel=False) -> List[List]:
        detailed_data = []
        for validation_result in self._results:
            detailed_data = detailed_data + self._generate_error_details(validation_result, excel)
        return sorted(
            detailed_data,
            key=lambda x: ((x[0], x[3]) if (self._item_type == "list") else (x["core_id"], x["dataset"])),
        )

    def _generate_error_details(self, validation_result: RuleValidationResult, excel) -> List[List]:
        """
        Generates the Issue details data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "CORE-ID",
            "Message",
            "Executability",
            "Dataset Name"
            "USUBJID",
            "Record",
            "Sequence",
            "Variable(s)",
            "Value(s)"
        ]
        """
        errors = []
        for result in validation_result.results or []:
            if result.get("errors", []) and result.get("executionStatus") == "success":
                variables = result.get("variables", [])
                for error in result.get("errors"):
                    error_item = {
                        "core_id": validation_result.id,
                        "message": result.get("message"),
                        "executability": validation_result.executability,
                        "dataset": error.get("dataset"),
                        "USUBJID": error.get("USUBJID", ""),
                        "row": error.get("row", ""),
                        "SEQ": error.get("SEQ", ""),
                    }
                    values = [str(error.get("value", {}).get(variable)) for variable in variables]
                    processed_values = self.process_values(values, excel)
                    if self._item_type == "list":
                        error_item["variables"] = ", ".join(variables)
                        error_item["values"] = processed_values
                        errors.append(list(error_item.values()))
                    elif self._item_type == "dict":
                        error_item["variables"] = variables
                        error_item["values"] = processed_values
                        errors.append(error_item)
        return errors

    def process_values(self, values: List[str], excel: bool) -> Union[str, List[str]]:
        if not values or values is None:
            return "null" if excel else ["null"]
        processed_values = []
        for value in values:
            value = value.strip()
            if value == "" or value.lower() == "none" or value.lower() == "nan":
                processed_values.append("null")
            else:
                processed_values.append(value)
        return ", ".join(processed_values) if excel else processed_values

    def get_rules_report_data(self) -> List[List]:
        """
        Generates the rules report data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        [
            "CORE-ID",
            "Version",
            "CDISC RuleID",
            "FDA RuleID",
            "Message",
            "Status",  # SUCCESS, ISSUE REPORTED, SKIPPED or EXECUTION ERROR
            "Reason"
        ]
        """
        rules_report = []
        for validation_result in self._results:
            status = self._get_rule_status(validation_result)
            rules_item = {
                "core_id": validation_result.id,
                "version": "1",
                "cdisc_rule_id": validation_result.cdisc_rule_id,
                "fda_rule_id": validation_result.fda_rule_id,
                "message": validation_result.message,
                "status": status.replace("_", " ").upper(),
                "skip_reason": self._get_skip_reason(validation_result, status),
            }
            if self._item_type == "list":
                rules_report.append([*rules_item.values()])
            elif self._item_type == "dict":
                rules_report.append(rules_item)

        return sorted(
            rules_report,
            key=lambda x: x[0] if (self._item_type == "list") else x["core_id"],
        )

    @staticmethod
    def _display_dataset_name(filename: Optional[str]) -> Optional[str]:
        if not filename:
            return filename
        return Path(filename).stem.upper()

    @staticmethod
    def _get_rule_status(validation_result) -> str:
        """
        Rule takes the highest-priority status among them:
        EXECUTION_ERROR > ISSUE_REPORTED > SUCCESS > SKIPPED.
        """
        results = [result for result in validation_result.results or [] if isinstance(result, dict)]
        statuses = {result.get("executionStatus") for result in results}
        if ExecutionStatus.EXECUTION_ERROR.value in statuses:
            return ExecutionStatus.EXECUTION_ERROR.value
        if any(
            result.get("errors") for result in results if result.get("executionStatus") == ExecutionStatus.SUCCESS.value
        ):
            return ExecutionStatus.ISSUE_REPORTED.value
        if validation_result.execution_status == ExecutionStatus.SUCCESS.value:
            return ExecutionStatus.SUCCESS.value
        return ExecutionStatus.SKIPPED.value

    @staticmethod
    def _get_skip_reason(validation_result, status: str) -> str:
        """
        SKIPPED rules list every reason a dataset was skipped. EXECUTION_ERROR rules list the errors
        first, then the datasets that reported issues, then the skip reasons.
        """
        if status not in (ExecutionStatus.SKIPPED.value, ExecutionStatus.EXECUTION_ERROR.value):
            return ""
        errors, issues, skips = {}, {}, {}
        for result in validation_result.results or []:
            if not isinstance(result, dict):
                continue
            dataset = BaseReport._display_dataset_name(result.get("dataset"))
            if result.get("executionStatus") == ExecutionStatus.SUCCESS.value:
                if result.get("errors"):
                    BaseReport._add_reason(issues, "Issues reported", dataset)
                continue
            reason = result.get("message")
            if not reason:
                continue
            match = _PER_DATASET_SKIP_REASON.match(reason)
            if match:
                BaseReport._add_reason(skips, match["reason"], match["dataset"].upper())
            elif result.get("executionStatus") == ExecutionStatus.EXECUTION_ERROR.value:
                error = next((error for error in result.get("errors") or [] if error.get("error")), {})
                detail = ": ".join(str(part) for part in (error.get("error"), error.get("message")) if part)
                BaseReport._add_reason(errors, f"{reason}: {detail}" if detail else reason, dataset)
            else:
                BaseReport._add_reason(skips, reason, None)
        return "; ".join(
            f"{reason} ({', '.join(datasets)})" if datasets else reason
            for reason, datasets in {**errors, **issues, **skips}.items()
        )

    @staticmethod
    def _add_reason(datasets_by_reason: dict, reason: str, dataset: Optional[str]) -> None:
        datasets = datasets_by_reason.setdefault(reason, [])
        if dataset and dataset not in datasets:
            datasets.append(dataset)

    @property
    @abstractmethod
    def _file_format(self) -> str:
        pass

    @abstractmethod
    def get_export(
        self,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions={},
        **kwargs,
    ) -> Union[dict, Workbook]:
        pass

    @abstractmethod
    def write_report(self, **kwargs):
        pass
