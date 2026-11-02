from unittest.mock import patch

from click.testing import CliRunner

from core import update_cache


def test_full_refresh_without_apikey_is_a_usage_error():
    with patch("core.CachePopulator") as populator:
        result = CliRunner().invoke(update_cache, [], env={"CDISC_LIBRARY_API_KEY": ""})
    assert result.exit_code == 2
    assert "--apikey" in result.output
    populator.return_value.update_cache.assert_not_called()


def test_custom_standard_removal_does_not_need_apikey():
    with patch("core.CachePopulator") as populator:
        result = CliRunner().invoke(update_cache, ["-rcs", "mycustom/1-0"], env={"CDISC_LIBRARY_API_KEY": ""})
    assert result.exit_code == 0, result.output
    populator.return_value.remove_custom_standards_from_cache.assert_called_once()
