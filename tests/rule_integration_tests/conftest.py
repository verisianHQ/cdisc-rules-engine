import pytest
import pickle


@pytest.fixture
def get_core_rule(pytestconfig) -> dict:
    def _call_core_rule(rule: str) -> dict:
        with open(str(pytestconfig.rootpath) + "/resources/cache/rules.pkl", "rb") as f:
            rules = pickle.load(f)
        return rules.get(rule)

    return _call_core_rule
