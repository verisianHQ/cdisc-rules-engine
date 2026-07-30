from types import SimpleNamespace

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
