from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class SqlOperationResult:
    """
    This class stores the output of a SQL operation.
    """

    query: str
    type: Literal["collection", "constant", "table"]
    data_type: Optional[Literal["text", "int", "float", "number", "numeric"]] = None
