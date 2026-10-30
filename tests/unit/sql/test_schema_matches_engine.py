"""
Keeps the rule schemas in resources/schema in line with what the SQL engine implements.
"""

import json
from pathlib import Path

from cdisc_rules_engine.check_operators.sql.postgresql_operators import PostgresQLOperators
from cdisc_rules_engine.compiler.sources import RULE_TYPE_SOURCES
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory

SCHEMA_DIR = Path(__file__).parents[3] / "resources" / "schema"

UNIMPLEMENTED_OPERATORS = {
    "conformant_value_data_type",
    "non_conformant_value_data_type",
    "conformant_value_length",
    "non_conformant_value_length",
    "references_correct_codelist",
    "does_not_reference_correct_codelist",
    "is_ordered_by",
    "is_not_ordered_by",
    "value_has_multiple_references",
    "value_does_not_have_multiple_references",
    "variable_metadata_equal_to",
    "variable_metadata_not_equal_to",
}


def _schema_operators(file_name: str) -> set:
    schema = json.loads((SCHEMA_DIR / file_name).read_text())
    return {entry["properties"]["operator"]["const"] for entry in schema["anyOf"]}


def test_operator_schema_matches_sql_operators():
    registered = set(PostgresQLOperators._operator_map)
    assert UNIMPLEMENTED_OPERATORS <= registered
    assert _schema_operators("Operator.json") == registered - UNIMPLEMENTED_OPERATORS


def test_operations_schema_matches_sql_operations():
    implemented = {name for name, operation in SqlOperationsFactory._operations_map.items() if operation is not None}
    assert _schema_operators("Operations.json") == implemented


def test_rule_type_schema_matches_compiler_sources():
    schema = json.loads((SCHEMA_DIR / "Rule_Type.json").read_text())
    # Record Data has no entry: it's the fallback for any rule type not in RULE_TYPE_SOURCES
    implemented = set(RULE_TYPE_SOURCES) | {"Record Data"}
    assert {entry["const"] for entry in schema["anyOf"]} == implemented
