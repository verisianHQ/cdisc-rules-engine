from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)
from cdisc_rules_engine.standards.sdtm_standards_context import SdtmStandardsContext


class StandardsFactory:
    _lookup = {"SDTMIG": SdtmStandardsContext}

    @staticmethod
    def get_standards_context(standard: str) -> BaseStandardsContext:
        constructor = StandardsFactory._lookup.get(standard.upper(), DefaultStandardsContext)
        return constructor()
