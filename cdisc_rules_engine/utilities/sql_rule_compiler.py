from typing import Any
from cdisc_rules_engine.check_operators.sql.postgresql_operators import PostgresQLOperators
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult


class SqlRuleCompiler:
    def __init__(self, data_service, dataset_id, dataset_metadata, operation_variables):
        self.data_service = data_service
        self.dataset_id = dataset_id
        self.schema: SqlTableSchema = data_service.pgi.schema.get_table(dataset_id)
        self.dataset_metadata = dataset_metadata
        self.operation_variables = operation_variables
        self.domain = dataset_metadata.domain or ""

    def compile(self, conditions: dict) -> str:
        sql = self._compile_node(conditions)
        return sql if sql else "FALSE"

    def _compile_node(self, node: dict) -> str:
        if not node:
            return "FALSE"

        if hasattr(node, "to_dict"):
            node = node.to_dict()

        if "all" in node:
            clauses = [self._compile_node(c) for c in node["all"]]
            return "(" + " AND ".join(clauses) + ")" if clauses else "TRUE"
        elif "any" in node:
            clauses = [self._compile_node(c) for c in node["any"]]
            return "(" + " OR ".join(clauses) + ")" if clauses else "FALSE"
        elif "not" in node:
            return f"NOT ({self._compile_node(node['not'])})"
        else:
            return self._compile_single_condition(node)

    def _compile_single_condition(self, condition: dict) -> str:
        value = condition.get("value", {})
        operator = condition.get("operator")

        operator_data = {
            "dataset_id": self.dataset_id,
            "data_service": self.data_service,
            "dataset_metadata": self.dataset_metadata,
            "operation_variables": self.operation_variables,
        }
        postgres_operators = PostgresQLOperators(operator_data)

        sql_condition_string = getattr(postgres_operators, operator)(value)
        return sql_condition_string

    def _resolve_operand(self, operand: Any, is_literal: bool = False) -> str:
        if operand is None:
            return "NULL"

        if isinstance(operand, str) and not is_literal:
            operand = operand.replace("--", self.domain)

            if operand.startswith("$"):
                op_result = self.operation_variables.get(operand)
                if op_result:
                    return self._get_op_result_query(op_result)

            if self.schema.has_column(operand):
                col_hash = self.schema.get_column_hash(operand)
                return f"co.{col_hash}"

        if isinstance(operand, str):
            return f"'{operand.replace(chr(39), chr(39) + chr(39))}'"
        if isinstance(operand, bool):
            return "TRUE" if operand else "FALSE"
        if isinstance(operand, (int, float)):
            return str(operand)

        return "NULL"

    def _get_op_result_query(self, op_result: SqlOperationResult) -> str:
        if op_result.type == "window":
            col_name = op_result.params.get("column_name")
            col_hash = self.schema.get_column_hash(col_name) or col_name
            return f"co.{col_hash}"
        elif op_result.type == "constant":
            return f"({op_result.query})"
        elif op_result.type == "collection":
            return f"(SELECT value FROM ({op_result.query}) as sub LIMIT 1)"
        else:
            raise TypeError(f"Unsupported operation result type: {op_result.type}")
