from types import SimpleNamespace

import pytest

from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import ConditionCompositeFactory
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor


def test_expand_rule_for_variable_regex_binds_condition_and_output_variable():
    conditions = ConditionCompositeFactory.get_condition_composite(
        {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "empty",
                    "value": {"target": "^AE.*DY$", "variable_regex_pattern": True},
                }
            ]
        }
    )
    rule = {
        "conditions": conditions,
        "output_variables": ["USUBJID", "^AE.*DY$"],
    }
    targets = [SimpleNamespace(name=name) for name in ["USUBJID", "AESTDY", "AEENDY", "AETERM"]]

    expanded_rules = SQLRuleProcessor.expand_rule_for_variable_regex(rule, targets)

    assert [expanded_rule["conditions"].values()[0]["value"]["target"] for expanded_rule in expanded_rules] == [
        "AESTDY",
        "AEENDY",
    ]
    assert [expanded_rule["output_variables"] for expanded_rule in expanded_rules] == [
        ["USUBJID", "AESTDY"],
        ["USUBJID", "AEENDY"],
    ]
    assert all(
        "variable_regex_pattern" not in expanded_rule["conditions"].values()[0]["value"]
        for expanded_rule in expanded_rules
    )


def test_expand_rule_for_variable_regex_retains_rule_when_no_targets_match():
    conditions = ConditionCompositeFactory.get_condition_composite(
        {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "empty",
                    "value": {"target": "^ZZ.*$", "variable_regex_pattern": True},
                }
            ]
        }
    )
    rule = {"conditions": conditions}

    expanded_rules = SQLRuleProcessor.expand_rule_for_variable_regex(rule, [SimpleNamespace(name="AESTDY")])

    assert expanded_rules == [rule]


def test_expand_rule_for_variable_regex_reuses_named_captures_throughout_rule():
    conditions = ConditionCompositeFactory.get_condition_composite(
        {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "equal_to",
                    "value": {
                        "target": r"^(?P<test>TEST(?P<number>[0-9]+))$",
                        "comparator": "{{test}}N",
                        "value_is_reference": True,
                        "variable_regex_pattern": True,
                    },
                },
                {
                    "any": [
                        {
                            "name": "get_dataset",
                            "operator": "not_empty",
                            "value": {"target": "{{test}}N"},
                        }
                    ]
                },
            ]
        }
    )
    rule = {
        "conditions": conditions,
        "operations": [{"id": "$result", "operator": "distinct", "name": "{{test}}N"}],
        "output_variables": ["USUBJID", "{{test}}", "{{test}}N"],
    }
    targets = [SimpleNamespace(name=name) for name in ["USUBJID", "TEST1", "TEST1N", "TEST20", "TEST20N"]]

    expanded_rules = SQLRuleProcessor.expand_rule_for_variable_regex(rule, targets)

    condition_values = [expanded_rule["conditions"].values()[0]["value"] for expanded_rule in expanded_rules]
    assert [(value["target"], value["comparator"]) for value in condition_values] == [
        ("TEST1", "TEST1N"),
        ("TEST20", "TEST20N"),
    ]
    assert [expanded_rule["conditions"].values()[1]["value"]["target"] for expanded_rule in expanded_rules] == [
        "TEST1N",
        "TEST20N",
    ]
    assert [expanded_rule["operations"][0]["name"] for expanded_rule in expanded_rules] == ["TEST1N", "TEST20N"]
    assert [expanded_rule["output_variables"] for expanded_rule in expanded_rules] == [
        ["USUBJID", "TEST1", "TEST1N"],
        ["USUBJID", "TEST20", "TEST20N"],
    ]


def test_expand_rule_for_variable_regex_rejects_unresolved_capture_reference():
    conditions = ConditionCompositeFactory.get_condition_composite(
        {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "empty",
                    "value": {
                        "target": r"^TEST(?P<number>[0-9]+)$",
                        "variable_regex_pattern": True,
                    },
                }
            ]
        }
    )
    rule = {"conditions": conditions, "output_variables": ["TEST{{missing}}"]}

    with pytest.raises(ValueError, match="Unresolved regex capture 'missing'"):
        SQLRuleProcessor.expand_rule_for_variable_regex(rule, [SimpleNamespace(name="TEST1")])


def test_expand_rule_for_variable_regex_rejects_duplicate_capture_names():
    conditions = ConditionCompositeFactory.get_condition_composite(
        {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "empty",
                    "value": {
                        "target": r"^TEST(?P<number>[0-9]+)$",
                        "variable_regex_pattern": True,
                    },
                },
                {
                    "name": "get_dataset",
                    "operator": "empty",
                    "value": {
                        "target": r"^VALUE(?P<number>[0-9]+)$",
                        "variable_regex_pattern": True,
                    },
                },
            ]
        }
    )
    rule = {"conditions": conditions}
    targets = [SimpleNamespace(name=name) for name in ["TEST1", "VALUE1"]]

    with pytest.raises(ValueError, match="Regex capture name 'number' is defined by multiple patterns"):
        SQLRuleProcessor.expand_rule_for_variable_regex(rule, targets)
