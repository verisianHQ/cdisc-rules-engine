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
