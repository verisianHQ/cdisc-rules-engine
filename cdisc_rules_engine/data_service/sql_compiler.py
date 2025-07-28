from typing import List


class SQLCompiler:
    """Multi-line compiler to combine multiple SQL statements"""

    @staticmethod
    def compile_statements(statements: List[str]) -> str:
        """Compile multiple SQL statements into a single execution string"""
        cleaned = [stmt.strip() for stmt in statements if stmt.strip()]

        normalized = []
        for stmt in cleaned:
            if not stmt.endswith(";"):
                stmt += ";"
            normalized.append(stmt)

        return "\n".join(normalized)
