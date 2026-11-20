from cdisc_rules_engine.compiler.compiler import RuleQueryCompiler
from cdisc_rules_engine.constants.metadata_columns import TOTAL_COUNT_COLUMN
from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import ConditionCompositeFactory
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext


def _visitdy_rule():
    return {
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {"all": [{"operator": "not_equal_to", "value": {"target": "VISITDY", "comparator": "TV.VISITDY"}}]}
        ),
        "output_variables": ["VISITDY", "TV.VISITDY"],
        "sensitivity": "Record",
    }


def _setup(join_type):
    ds = PostgresQLDataService.instance()
    ae = PostgresQLDataService.add_test_dataset(
        ds,
        "ae",
        {
            "STUDYID": ["S1", "S1", "S1"],
            "DOMAIN": ["AE", "AE", "AE"],
            "USUBJID": ["U1", "U2", "U3"],
            "VISITNUM": [1, 1, 2],
            "VISITDY": [1, 5, 8],
        },
        DefaultStandardsContext(),
    )
    tv = PostgresQLDataService.add_test_dataset(
        ds,
        "tv",
        {"STUDYID": ["S1", "S1", "S1"], "VISITNUM": [1, 1, 2], "ARMCD": ["A", "B", "A"], "VISITDY": [1, 3, 8]},
        DefaultStandardsContext(),
    )
    joined = SqlJoinMerge.perform_join(
        pgi=ds.pgi, left=ae, right=tv, pivot_left=["VISITNUM"], pivot_right=["VISITNUM"], type=join_type
    )
    compiled = RuleQueryCompiler(_visitdy_rule(), ds, ds.get_dataset_metadata("ae"), joined.name).compile()
    ds.pgi.execute_sql(compiled.sql)
    return joined, compiled, ds.pgi.fetch_all()


def test_inner_join_rule_reports_each_left_row_once():
    joined, compiled, rows = _setup("INNER")

    assert "EXISTS" in compiled.sql
    assert joined.hash not in compiled.sql
    tv_visitdy = joined.get_column_hash("TV.VISITDY")
    by_subject = {row[joined.get_column_hash("USUBJID")]: row for row in rows}
    assert sorted(by_subject) == ["U1", "U2"]
    assert by_subject["U1"][tv_visitdy] == 3
    assert by_subject["U2"][tv_visitdy] in (1, 3)
    assert all(row[TOTAL_COUNT_COLUMN] == 2 for row in rows)


def test_left_join_rule_keeps_joined_rows():
    joined, compiled, rows = _setup("LEFT")

    assert "EXISTS" not in compiled.sql
    assert len(rows) == 3
