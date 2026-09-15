from cdisc_rules_engine.check_operators.sql import PostgresQLOperators

from .helpers import create_sql_operators


def test_regex_target_is_missing_only_when_no_columns_match():
    sql_operators = create_sql_operators({"AESTDY": [1], "AEENDY": [2], "AETERM": ["Headache"]})
    operator_instance = sql_operators._operator_map["empty"](sql_operators.data)

    matching = PostgresQLOperators._missing_columns_for_operator(
        "empty",
        operator_instance,
        {"target": "^AE.*DY$", "variable_regex_pattern": True},
    )
    missing = PostgresQLOperators._missing_columns_for_operator(
        "empty",
        operator_instance,
        {"target": "^ZZ.*$", "variable_regex_pattern": True},
    )

    assert matching == []
    assert missing == ["^ZZ.*$"]
