from rule_regression.regression import extract_results_regression


def test_extract_results_regression_merges_regex_expansions_for_dataset():
    results = {
        "adsl": [
            {
                "dataset": "adsl.csv",
                "domain": "adsl",
                "executionStatus": "success",
                "message": "Inconsistent treatment mapping",
                "errors": [{"row": 1, "value": {"TRT01AN": 1, "TRT01A": "X"}}],
            },
            {
                "dataset": "adsl.csv",
                "domain": "adsl",
                "executionStatus": "success",
                "message": "Inconsistent treatment mapping",
                "errors": [{"row": 1, "value": {"TRT02AN": 1, "TRT02A": "X"}}],
            },
        ]
    }

    regression = extract_results_regression(results)

    assert regression[0]["number_errors"] == 2
    assert regression[0]["errors"] == [
        {"row": 1, "SEQ": None, "USUBJID": None, "value": {"TRT01AN": 1, "TRT01A": "X"}},
        {"row": 1, "SEQ": None, "USUBJID": None, "value": {"TRT02AN": 1, "TRT02A": "X"}},
    ]


def test_extract_results_regression_keeps_split_source_datasets_separate():
    results = {
        "ae": [
            {
                "dataset": "ae1.xpt",
                "domain": "AE",
                "executionStatus": "success",
                "message": "Missing AETERM",
                "errors": [{"row": 1, "value": {"AETERM": None}}],
            },
            {
                "dataset": "ae2.xpt",
                "domain": "AE",
                "executionStatus": "success",
                "message": "Missing AETERM",
                "errors": [{"row": 2, "value": {"AETERM": None}}],
            },
        ]
    }

    regression = extract_results_regression(results)

    assert [entry["dataset"] for entry in regression] == ["ae1.xpt", "ae2.xpt"]
    assert [entry["number_errors"] for entry in regression] == [1, 1]


def test_extract_results_regression_keeps_clean_split_source_dataset():
    results = {
        "mh": [
            {
                "dataset": "mh1.xpt",
                "domain": "MH",
                "executionStatus": "success",
                "message": "Duplicate MHSEQ",
                "errors": [
                    {"row": 1, "value": {"MHSEQ": 1}},
                    {"row": 2, "value": {"MHSEQ": 1}},
                ],
            },
            {
                "dataset": "mh2.xpt",
                "domain": "MH",
                "executionStatus": "success",
                "message": "Duplicate MHSEQ",
                "errors": [],
            },
        ]
    }

    regression = extract_results_regression(results)

    assert [entry["dataset"] for entry in regression] == ["mh1.xpt", "mh2.xpt"]
    assert [entry["number_errors"] for entry in regression] == [2, 0]
    assert regression[1]["errors"] == []
