import glob
import os
import re

import pytest
from cdisc_rules_engine.enums.optional_condition_parameters import (
    OptionalConditionParameters,
)
from cdisc_rules_engine.models.sql_rule import SQLRule


@pytest.mark.parametrize(
    "condition, expected_additional_keys",
    [
        ({"operator": "test", "name": "IDVAR", "prefix": 10}, ["prefix"]),
        ({"operator": "test", "name": "IDVAR", "suffix": 10}, ["suffix"]),
        (
            {"operator": "test", "name": "IDVAR", "date_component": "year"},
            ["date_component"],
        ),
        ({"operator": "test", "name": "IDVAR", "context": "RDOMAIN"}, ["context"]),
        (
            {"operator": "test", "name": "IDVAR", "value_is_literal": False},
            ["value_is_literal"],
        ),
        (
            {"operator": "test", "name": "IDVAR", "metadata": "metadata_column"},
            ["metadata"],
        ),
        (
            {"operator": "test", "name": "^ID.*", "variable_regex_pattern": True},
            ["variable_regex_pattern"],
        ),
        (
            {
                "operator": "test",
                "name": "IDVAR",
                "within": "metadata_column",
                "order": "asc",
                "ordering": "asc",
            },
            ["within", "order", "ordering"],
        ),
        (
            {"operator": "test", "name": "IDVAR", "where_populated": True},
            ["where_populated"],
        ),
        (
            {
                "operator": "test",
                "name": "IDVAR",
                "where_populated_columns": ["OTHER"],
            },
            ["where_populated_columns"],
        ),
    ],
)
def test_build_conditions(condition, expected_additional_keys):
    result = SQLRule.build_condition(condition, "get_dataset")
    value = result.get("value")
    assert len(value.keys()) == 2 + len(expected_additional_keys)
    assert value["target"] == condition["name"]
    for key in expected_additional_keys:
        assert value[key] == condition[key]


def _find_operator_condition_keys() -> set:
    keys = set()
    condition_key_pattern = re.compile(r'other_value(?:\.get\(|\[)"([a-zA-Z_]+)"')
    sql_operator_files = glob.glob(
        os.path.join("cdisc_rules_engine", "check_operators", "sql", "**", "*.py"), recursive=True
    )
    for path in sql_operator_files:
        with open(path, "r") as f:
            found_keys = condition_key_pattern.findall(f.read())
            keys.update(found_keys)
    return keys


def test_all_sql_operator_condition_keys_are_optional_condition_parameters():
    core_condition_keys = {
        "target",
        "comparator",
        "negative",
        "regex",
        "length",
    }
    known_keys = core_condition_keys | set(OptionalConditionParameters.values())
    operator_keys = _find_operator_condition_keys()

    missing_keys = operator_keys - known_keys
    assert not missing_keys
