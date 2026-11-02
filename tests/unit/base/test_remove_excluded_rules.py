from scripts.script_utils import remove_excluded_rules


def test_remove_excluded_rules_is_case_insensitive():
    rules = [{"core_id": "CORE-000001"}, {"core_id": "CORE-000002"}, {"core_id": "CORE-000003"}]
    assert remove_excluded_rules(rules, ("core-000001", "CORE-000003", "CORE-999999")) == [{"core_id": "CORE-000002"}]
    assert remove_excluded_rules(rules, ()) == rules
