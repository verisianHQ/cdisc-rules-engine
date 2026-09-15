import hashlib
import base64
import re


def generate_hash(input_string: str, length: int = 10) -> str:
    hash_bytes = hashlib.sha256(input_string.encode("utf-8")).digest()
    hash_b64 = base64.urlsafe_b64encode(hash_bytes).decode("utf-8")
    alphanum_hash = re.sub(r"[^a-zA-Z0-9]", "", hash_b64).lower()

    clean_input = re.sub(r"[^a-zA-Z0-9]", "_", input_string).lower()
    if not clean_input or not clean_input[0].isalpha():
        clean_input = "v_" + clean_input
    clean_input = clean_input[:50]

    return f"{clean_input}_{alphanum_hash[:length]}"


# Matches a plain or scientific-notation numeric literal (e.g. "1", "-1.5", "1E5", "1e+15").
NUMERIC_LITERAL_PATTERN = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"


def safe_numeric_cast_sql(value_sql: str) -> str:
    """
    Safely cast a SQL expression to NUMERIC, returning NULL instead of raising a Postgres
    cast error when the underlying text isn't a valid number. Recognizes scientific
    notation, so a value like '1E5' is treated as numeric.
    """
    return f"""CASE
            WHEN TRIM(CAST({value_sql} AS TEXT)) ~ '{NUMERIC_LITERAL_PATTERN}'
                THEN CAST(TRIM(CAST({value_sql} AS TEXT)) AS NUMERIC)
            ELSE NULL
        END"""


def numeric_aware_equals_sql(left_sql: str, right_sql: str) -> str:
    """
    Compare two SQL expressions for equality, preferring a numeric comparison when both
    sides parse as numbers and falling back to a plain text comparison otherwise.
    """
    left_numeric = safe_numeric_cast_sql(left_sql)
    right_numeric = safe_numeric_cast_sql(right_sql)
    return f"""(
        CASE
            WHEN ({left_numeric}) IS NOT NULL AND ({right_numeric}) IS NOT NULL
                THEN ({left_numeric}) = ({right_numeric})
            ELSE CAST({left_sql} AS TEXT) = CAST({right_sql} AS TEXT)
        END
    )"""
