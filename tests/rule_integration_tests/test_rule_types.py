from unittest.mock import patch

from scripts.run_validation import run_single_rule_validation


def test_test(get_core_rule):
    rule = get_core_rule("CORE-000254")
    assert rule is not None

    # data = validate_single_rule(datasets, rule)


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_existing_rule(mock_get_dataset_class):
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "variables": [
                {
                    "name": "DOMAIN",
                    "label": "Domain Abbreviation",
                    "type": "Char",
                    "length": 4,
                },
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
            ],
            "records": {
                "DOMAIN": ["LB", "LB"],
                "LBSEQ": [1, 2],
            },
        }
    ]
    rule = {
        "core_id": "QC.CDISC.SDTMIG.CG0032",
        "classes": {"Include": ["ALL"]},
        "domains": {"Include": ["ALL"]},
        "rule_type": "Range & Limit",
        "sensitivity": "Record",
        "severity": "error",
        "Authorities": [{"Standards": [{"Name": "SDTMIG", "Version": "3.4"}]}],
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "greater_than",
                    "value": {"target": "LBSEQ", "comparator": 0},
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "LBSEQ greater than 0"},
            }
        ],
    }
    mock_get_dataset_class.return_value = None
    data = run_single_rule_validation(datasets, rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert data["LB"][0]["message"] == "LBSEQ greater than 0"
    assert len(data["LB"][0]["errors"]) == 2
