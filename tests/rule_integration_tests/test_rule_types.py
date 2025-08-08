from unittest.mock import patch

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from scripts.run_sql_validation import sql_run_single_rule_validation


def test_regression(pytestconfig, get_core_rules_df, get_core_rule):
    regression_df = get_core_rules_df()

    # regression fields
    regression_df["core_id_is_null"] = False
    regression_df["has_core_structure"] = True
    regression_df["in_cache"] = True

    # tests
    regression_df["core_execute"] = False
    regression_df["core_unit_test_pass"] = False
    regression_df["core_time"] = 0
    regression_df["sql_core_execute"] = False
    regression_df["sql_core_unit_test_pass"] = False
    regression_df["sql_core_time"] = 0
    regression_df["sql_core_time_delta"] = 0

    for idx, row in regression_df.iterrows():
        cur_core_id = str(row["Core-ID"])
        if cur_core_id:
            if cur_core_id.startswith("CORE-"):
                rule = get_core_rule(cur_core_id)
                if rule:
                    print("rule is not none")
                    assert rule is not None
                else:
                    regression_df.at[idx, "in_cache"] = False
            else:
                regression_df.at[idx, "has_core_structure"] = False
                regression_df.at[idx, "in_cache"] = False
        else:
            regression_df["core_id_is_null"] = True
            regression_df.at[idx, "has_core_structure"] = False
            regression_df.at[idx, "in_cache"] = False

    # data = validate_single_rule(datasets, rule)

    rule = get_core_rule("CORE-000254")
    assert rule is not None

    output_df = regression_df.drop(columns=["Description", "Standard Version", "Scope", "std", "rids"], errors="ignore")
    output_df.to_json(
        str(pytestconfig.rootpath) + "/tests/resources/rules/rules.json",
        orient="records",
        indent=2,
    )


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_existing_rule(mock_get_dataset_class, get_sample_lb_rule, get_sample_lb_dataset):
    mock_get_dataset_class.return_value = None
    ig_specs = {
        "standard": "SDTMIG",
        "standard_version": "3.4",
        "standard_substandard": None,
        "define_xml_version": None,
    }
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], ig_specs)
    data = sql_run_single_rule_validation(data_service=ds, rule=get_sample_lb_rule)

    assert "LB" in data
    assert len(data["LB"]) == 1
    assert data["LB"][0]["message"] == "LBSEQ greater than 0"
    assert len(data["LB"][0]["errors"]) == 2
