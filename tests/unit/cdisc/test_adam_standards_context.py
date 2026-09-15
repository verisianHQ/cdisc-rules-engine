import pytest

from cdisc_rules_engine.constants.data_structures import ADSL, BDS, OCCDS, OTHER
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2, VariableMetadata
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.standards.adam_standards_context import AdamStandardsContext


def _metadata(name: str, columns: list[str]) -> DatasetMetadata2:
    return DatasetMetadata2(
        filename=f"{name.lower()}.xpt",
        name=name,
        label=name,
        variables=[
            VariableMetadata(name=column, label=column, type="Char", length=8, format="", order=index)
            for index, column in enumerate(columns, start=1)
        ],
    )


@pytest.mark.parametrize(
    "name,columns,expected_class",
    [
        ("ADSL", ["STUDYID", "USUBJID"], ADSL),
        ("ADVS", ["STUDYID", "USUBJID", "PARAMCD", "AVAL"], BDS),
        ("ADAE", ["STUDYID", "USUBJID", "AETERM"], OCCDS),
        ("ADCUSTOM", ["STUDYID", "USUBJID", "CUSTOM"], OTHER),
    ],
)
def test_transform_dataset_metadata_assigns_adam_class(name, columns, expected_class):
    context = AdamStandardsContext(LibraryMetadataContainer())

    transformed = context.transform_dataset_metadata(_metadata(name, columns))

    assert transformed.dataset_class == expected_class


@pytest.mark.parametrize(
    "rule,expected",
    [
        ({"data_structures": {"Include": [BDS]}}, True),
        ({"data_structures": {"Include": ["BDS"]}}, True),
        ({"data_structures": {"Include": [OCCDS]}}, False),
        ({"data_structures": {"Include": ["OCCDS"]}}, False),
        ({"data_structures": {"Include": ["all"]}}, True),
        ({"data_structures": {"Exclude": [BDS]}}, False),
        ({"data_structures": {"Exclude": ["BDS"]}}, False),
        ({"data_structures": {"Exclude": [OCCDS]}}, True),
        ({"classes": {"Include": [BDS]}}, True),
        ({"classes": {"Include": ["BDS"]}}, True),
    ],
)
def test_adam_rule_applies_to_class(rule, expected):
    context = AdamStandardsContext(LibraryMetadataContainer())
    metadata = context.transform_dataset_metadata(_metadata("ADVS", ["USUBJID", "PARAMCD", "AVAL"]))

    assert context.rule_applies_to_class(metadata, rule) is expected


def test_adam_scope_requires_both_data_structure_and_domain():
    context = AdamStandardsContext(LibraryMetadataContainer())
    metadata = context.transform_dataset_metadata(_metadata("ADVS", ["USUBJID", "PARAMCD", "AVAL"]))
    matching_rule = {
        "core_id": "TEST",
        "data_structures": {"Include": [BDS]},
        "domains": {"Include": ["ADVS"]},
    }
    wrong_domain_rule = {**matching_rule, "domains": {"Include": ["ADAE"]}}

    assert context.within_rule_scope(matching_rule, metadata) == (True, "")
    assert context.within_rule_scope(wrong_domain_rule, metadata)[0] is False
