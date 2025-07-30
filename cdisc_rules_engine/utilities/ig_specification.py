from typing import TypedDict, Optional


class IGSpecification(TypedDict):
    standard: Optional[str]
    standard_version: str
    standard_substandard: Optional[str]
