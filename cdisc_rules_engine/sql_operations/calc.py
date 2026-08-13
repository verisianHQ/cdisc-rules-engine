import re

from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError, RuleExecutionError
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlCalcOperation(SqlBaseOperation):
    _NUMERIC_CAST_REGEX = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"

    _TOKEN_RE = re.compile(
        r"""
          (?P<WS>\s+)
        | (?P<NUMBER>\d+\.\d+|\.\d+|\d+)
        | (?P<OPREF>\$[A-Za-z_][A-Za-z0-9_]*)
        | (?P<IDENT>[A-Za-z_][A-Za-z0-9_]*)
        | (?P<OP>[+\-*/()])
        """,
        re.VERBOSE,
    )

    def _execute_operation(self):
        formula = self.params.value
        if formula is None or not str(formula).strip():
            raise RuleExecutionError("calc operation requires a non-empty 'value' formula")

        self._params: dict[str, str] = {}
        self._column_placeholders: dict[str, str] = {}
        self._opref_fragments: dict[str, str] = {}
        self._placeholder_counter = 0

        tokens = self._tokenie(str(formula))
        self._tokens = tokens
        self._pos = 0

        expression_sql = self._parse_expression()
        if self._pos != len(self._tokens):
            unexpected = self._tokens[self._pos][1]
            raise RuleExecutionError(f"Unexpected token '{unexpected}' in calc formula '{formula}'")

        query = f"SELECT {expression_sql} AS value"
        return SqlOperationResult(
            query=query,
            type="constant",
            subtype="Num",
            params=self._params or None,
        )

    def _tokenie(self, formula: str):
        tokens = []
        pos = 0
        length = len(formula)
        while pos < length:
            match = self._TOKEN_RE.match(formula, pos)
            if not match:
                raise RuleExecutionError(f"Invalid character '{formula[pos]}' in calc formula '{formula}'")
            kind = match.lastgroup
            value = match.group()
            pos = match.end()
            if kind == "WS":
                continue
            tokens.append((kind, value))
        return tokens

    def _peek(self):
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return (None, "")

    def _advance(self):
        token = self._tokens[self._pos]
        self._pos += 1
        return token

    def _parse_expression(self) -> str:
        node = self._parse_term()
        while self._peek() == ("OP", "+") or self._peek() == ("OP", "-"):
            operator = self._advance()[1]
            right = self._parse_term()
            node = f"({node} {operator} {right})"
        return node

    def _parse_term(self) -> str:
        node = self._parse_factor()
        while self._peek() == ("OP", "*") or self._peek() == ("OP", "/"):
            operator = self._advance()[1]
            right = self._parse_factor()
            if operator == "/":
                node = f"({node} / NULLIF({right}, 0))"
            else:
                node = f"({node} * {right})"
        return node

    def _parse_factor(self) -> str:
        token = self._peek()
        if token == ("OP", "+"):
            self._advance()
            return self._parse_factor()
        if token == ("OP", "-"):
            self._advance()
            return f"(-{self._parse_factor()})"
        return self._parse_primary()

    def _parse_primary(self) -> str:
        kind, value = self._peek()
        if kind is None:
            raise RuleExecutionError(f"Unexpected end of calc formula '{self.params.value}'")
        if value == "(":
            self._advance()
            node = self._parse_expression()
            if self._peek() != ("OP", ")"):
                raise RuleExecutionError(f"Missing closing parenthesis in calc formula '{self.params.value}'")
            self._advance()
            return f"({node})"
        if kind == "NUMBER":
            self._advance()
            return f"({value})"
        if kind == "OPREF":
            self._advance()
            return self._resolve_opref(value)
        if kind == "IDENT":
            self._advance()
            return self._resolve_column(value)
        raise RuleExecutionError(f"Unexpected token '{value}' in calc formula '{self.params.value}'")

    def _next_placeholder(self, prefix: str) -> str:
        placeholder = f"$calc_{prefix}{self._placeholder_counter}$"
        self._placeholder_counter += 1
        return placeholder

    def _resolve_column(self, name: str) -> str:
        if name in self._column_placeholders:
            return self._column_placeholders[name]

        column = self.data_service.pgi.schema.get_column(self.params.domain, name)
        if column is None:
            raise ColumnNotFoundError(column_name=name, table_id=self.params.domain)

        placeholder = self._next_placeholder("c")
        self._params[placeholder] = column.name

        if column.type == "Num":
            fragment = f"({placeholder})"
        else:
            fragment = f"({self._safe_numeric_cast(placeholder)})"
        self._column_placeholders[name] = fragment
        return fragment

    def _resolve_opref(self, name: str) -> str:
        if name in self._opref_fragments:
            return self._opref_fragments[name]

        previous_operations = self.params.previous_operations or {}
        op_result = previous_operations.get(name)
        if op_result is None:
            raise RuleExecutionError(
                f"Operation reference '{name}' in calc formula is not a known previous operation result"
            )
        if op_result.type != "constant":
            raise RuleExecutionError(
                f"Operation reference '{name}' in calc formula must be a constant (scalar) result, "
                f"got '{op_result.type}'"
            )

        renamed_query = op_result.query
        for placeholder, column_name in sorted(
            (op_result.params or {}).items(), key=lambda item: len(item[0]), reverse=True
        ):
            new_placeholder = self._next_placeholder("o")
            renamed_query = renamed_query.replace(placeholder, new_placeholder)
            self._params[new_placeholder] = column_name

        if op_result.subtype == "Num":
            fragment = f"({renamed_query})"
        else:
            fragment = f"({self._safe_numeric_cast(f'({renamed_query})')})"
        self._opref_fragments[name] = fragment
        return fragment

    def _safe_numeric_cast(self, expr: str) -> str:
        return (
            f"CASE WHEN TRIM(CAST({expr} AS TEXT)) ~ '{self._NUMERIC_CAST_REGEX}' "
            f"THEN CAST(TRIM(CAST({expr} AS TEXT)) AS NUMERIC) ELSE NULL END"
        )
