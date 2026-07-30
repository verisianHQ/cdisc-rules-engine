from types import SimpleNamespace
from unittest.mock import patch

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
