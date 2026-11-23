import json
import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from cdisc_rules_engine.services.cache.cache_populator_service import CachePopulator
from cdisc_rules_engine.utilities.utils import load_json_with_optional_encoding
from core import update_cache
from scripts.script_utils import load_and_parse_rule

STANDARD = {"日本標準/1-0": ["CUSTOM-001", "CORE-000001"]}
BOM = b"\xef\xbb\xbf"


def write(tmp_path, data: bytes, name="standard.json") -> str:
    path = tmp_path / name
    path.write_bytes(data)
    return str(path)


def utf8_bom_file(tmp_path) -> str:
    return write(tmp_path, BOM + json.dumps(STANDARD, ensure_ascii=False).encode("utf-8"))


def cp932_file(tmp_path) -> str:
    return write(tmp_path, json.dumps(STANDARD, ensure_ascii=False).encode("cp932"))


def cached_standards(cache_path) -> dict:
    with open(Path(cache_path) / "custom_rules_dictionary.pkl", "rb") as f:
        return pickle.load(f)


def test_utf8_with_bom_loads_without_encoding(tmp_path):
    with patch("locale.getpreferredencoding", return_value="cp932"):
        assert load_json_with_optional_encoding(utf8_bom_file(tmp_path)) == STANDARD


def test_explicit_encoding_is_used(tmp_path):
    assert load_json_with_optional_encoding(cp932_file(tmp_path), "cp932") == STANDARD


def test_falls_back_to_system_encoding(tmp_path):
    with patch("locale.getpreferredencoding", return_value="cp932"):
        assert load_json_with_optional_encoding(cp932_file(tmp_path)) == STANDARD


def test_falls_back_when_explicit_encoding_is_wrong(tmp_path):
    assert load_json_with_optional_encoding(utf8_bom_file(tmp_path), "utf-8") == STANDARD


def test_invalid_json_reports_every_attempt(tmp_path):
    path = write(tmp_path, b'{"a": ')
    with patch("locale.getpreferredencoding", return_value="cp932"):
        with pytest.raises(ValueError, match="utf-8-sig: .*cp932: "):
            load_json_with_optional_encoding(path)


@pytest.mark.parametrize("make_file,encoding", [(utf8_bom_file, None), (cp932_file, "cp932")])
def test_add_custom_standard_to_cache(tmp_path, make_file, encoding):
    populator = CachePopulator(
        MagicMock(),
        custom_standards=make_file(tmp_path),
        custom_standards_encoding=encoding,
        cache_path=str(tmp_path),
    )
    populator.add_custom_standard_to_cache()
    assert cached_standards(tmp_path) == STANDARD


def test_cli_passes_custom_standard_encoding(tmp_path):
    result = CliRunner().invoke(
        update_cache,
        ["-c", str(tmp_path), "-cs", cp932_file(tmp_path), "-cse", "cp932"],
        env={"CDISC_LIBRARY_API_KEY": ""},
    )
    assert result.exit_code == 0, result.output
    assert cached_standards(tmp_path) == STANDARD


def test_cli_rejects_unknown_encoding(tmp_path):
    with patch("core.CachePopulator") as populator:
        result = CliRunner().invoke(
            update_cache,
            ["-cs", cp932_file(tmp_path), "-cse", "not-a-codec"],
            env={"CDISC_LIBRARY_API_KEY": ""},
        )
    assert result.exit_code == 2
    assert "not-a-codec" in result.output
    populator.assert_not_called()


def test_json_rule_with_bom_loads(tmp_path):
    source = Path(__file__).parents[2] / "resources" / "Rule-CG0027.json"
    path = write(tmp_path, BOM + source.read_bytes(), name="rule.json")
    assert load_and_parse_rule(path) is not None
