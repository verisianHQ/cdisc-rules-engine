import pytest

from types import SimpleNamespace
from unittest.mock import patch

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.exceptions.custom_exceptions import (
    ColumnNotFoundError,
    DomainNotFoundError,
    InvalidMatchKeyError,
    MatchDatasetNotFoundError,
)
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import ConditionCompositeFactory
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine


def test_execute_rule_runs_once_per_regex_variable():
    rule = {
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "operator": "empty",
                        "value": {"target": "^AE.*DY$", "variable_regex_pattern": True},
                    }
                ]
            }
        ),
        "output_variables": ["^AE.*DY$"],
    }
    metadata = SimpleNamespace(variables=[SimpleNamespace(name=name) for name in ["AESTDY", "AEENDY", "AETERM"]])
    engine = SQLRulesEngine.__new__(SQLRulesEngine)

    with patch.object(engine, "_execute_expanded_rule", return_value=[]) as execute:
        engine.execute_rule(rule, metadata, "ae")

    expanded_rules = [call.args[0] for call in execute.call_args_list]
    assert [expanded_rule["output_variables"] for expanded_rule in expanded_rules] == [
        ["AESTDY"],
        ["AEENDY"],
    ]


def test_missing_match_key_skips_but_typo_fails():
    engine = SQLRulesEngine.__new__(SQLRulesEngine)

    skipped = engine.handle_validation_exceptions(ColumnNotFoundError("USUBJID", "ts"), "ts.xpt")
    failed = engine.handle_validation_exceptions(
        InvalidMatchKeyError("Match key USUBID is not a variable in any dataset in the study"), "ae.xpt"
    )

    assert skipped.status == ExecutionStatus.SKIPPED.value
    assert failed.status == ExecutionStatus.EXECUTION_ERROR.value
    assert failed.to_representation()["errors"] == [
        {
            "dataset": "ae.xpt",
            "error": "Invalid match key provided",
            "message": "Match key USUBID is not a variable in any dataset in the study",
        }
    ]


@pytest.mark.parametrize(
    "exception, expected",
    [(KeyError("B, A"), "A, B"), (KeyError(None), "None"), (KeyError(), "")],
)
def test_key_error_without_a_string_message_is_reported_not_raised(exception, expected):
    engine = SQLRulesEngine.__new__(SQLRulesEngine)

    result = engine.handle_validation_exceptions(exception, "ae.xpt")

    assert result.status == ExecutionStatus.EXECUTION_ERROR.value
    assert result.errors[0].to_representation()["message"] == expected


@pytest.mark.parametrize(
    "exception, expected_message",
    [
        (
            MatchDatasetNotFoundError("Match dataset SS is not present"),
            "rule evaluation skipped - match dataset not found",
        ),
        (DomainNotFoundError("Domain DM not found"), "rule evaluation skipped - operation domain not found"),
    ],
)
def test_missing_dataset_skip_message(exception, expected_message):
    engine = SQLRulesEngine.__new__(SQLRulesEngine)

    result = engine.handle_validation_exceptions(exception, "ae.xpt")

    assert result.status == ExecutionStatus.SKIPPED.value
    assert result.message == expected_message
