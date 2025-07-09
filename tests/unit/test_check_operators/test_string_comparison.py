from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,comparator,regex,dataset_type,expected_result",
    [
        (
            {"VAR2": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "D"]},
            "VAR2",
            ".{3}(.).*",
            PandasDataset,
            [True, True, False],
        ),
        (
            {"VAR2": ["blaAtt", "yyyBtt", "aaaCtt"], "target": ["A", "B", "D"]},
            "VAR2",
            ".{3}(.).*",
            DaskDataset,
            [True, True, False],
        ),
    ],
)
def test_equals_string_part(data, comparator, regex, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.equals_string_part({"target": "target", "comparator": comparator, "regex": regex})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "B", "D"]},
            "VAR2",
            PandasDataset,
            [True, True, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "B", "D"]},
            "VAR2",
            DaskDataset,
            [True, True, False],
        ),
    ],
)
def test_starts_with(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.starts_with({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["tt", "B", "D"]},
            "VAR2",
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["tt", "B", "D"]},
            "VAR2",
            DaskDataset,
            [True, True, True],
        ),
    ],
)
def test_ends_with(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.ends_with({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            DaskDataset,
            [False, False, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            PandasDataset,
            [False, True, False],
        ),
    ],
)
def test_has_equal_length(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.has_equal_length({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            PandasDataset,
            [True, True, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            DaskDataset,
            [False, False, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            DaskDataset,
            [True, False, True],
        ),
    ],
)
def test_has_not_equal_length(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.has_not_equal_length({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            DaskDataset,
            [True, True, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctta"], "VAR2": ["A", "Bd", "lll"]},
            3,
            PandasDataset,
            [False, False, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            PandasDataset,
            [True, False, True],
        ),
    ],
)
def test_longer_than(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.longer_than({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["AiAa", "Bd", "lll"]},
            "VAR2",
            PandasDataset,
            [False, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            DaskDataset,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            DaskDataset,
            [True, True, True],
        ),
    ],
)
def test_longer_than_or_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.longer_than_or_equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            "VAR2",
            DaskDataset,
            [False, False, False],
        ),
        (
            {"target": ["At", "Btt", "Ctta"], "VAR2": ["A", "Bd", "lll"]},
            3,
            PandasDataset,
            [True, False, False],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 5, 2]},
            "VAR2",
            PandasDataset,
            [False, True, False],
        ),
    ],
)
def test_shorter_than(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.shorter_than({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["AiAa", "Bd", "lll"]},
            "VAR2",
            DaskDataset,
            [True, False, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
            3,
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
            "VAR2",
            PandasDataset,
            [False, True, False],
        ),
    ],
)
def test_shorter_than_or_equal_to(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.shorter_than_or_equal_to({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["Att", "", None, {None}, {None, 1}, {1, 2}]},
            PandasDataset,
            [False, True, True, True, False, False],
        ),
        (
            {"target": ["Att", "", None, {None}, {None, 1}, {1, 2}]},
            DaskDataset,
            [False, True, True, True, False, False],
        ),
    ],
)
def test_empty(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.empty({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        ({"target": ["Att", "", None]}, PandasDataset, [True, False, False]),
        ({"target": ["Att", "", None]}, DaskDataset, [True, False, False]),
    ],
)
def test_non_empty(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.non_empty({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,dataset_type,expected_result",
    [
        (
            {
                "target": ["word", "TEST"],
            },
            r"w.*",
            2,
            PandasDataset,
            [True, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            r"[0-9].*",
            2,
            DaskDataset,
            [False, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [True, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [True, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [False, False],
        ),
    ],
)
def test_prefix_matches_regex(data, comparator, prefix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.prefix_matches_regex({"target": "target", "comparator": comparator, "prefix": prefix})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,suffix,dataset_type,expected_result",
    [
        (
            {
                "target": ["WORD", "test"],
            },
            r"es.*",
            3,
            DaskDataset,
            [False, True],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            r"[0-9].*",
            3,
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [True, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [True, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [True, True],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_suffix_matches_regex(data, comparator, suffix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.suffix_matches_regex({"target": "target", "comparator": comparator, "suffix": suffix})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,suffix,dataset_type, expected_result",
    [
        (
            {
                "target": ["WORD", "test"],
            },
            r".*",
            3,
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            r"[0-9].*",
            3,
            DaskDataset,
            [True, True],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [False, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [False, False],
        ),
    ],
)
def test_not_suffix_matches_regex(data, comparator, suffix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_suffix_matches_regex({"target": "target", "comparator": comparator, "suffix": suffix})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,prefix,dataset_type, expected_result",
    [
        (
            {
                "target": ["word", "TEST"],
            },
            r".*",
            2,
            DaskDataset,
            [False, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            r"[0-9].*",
            2,
            PandasDataset,
            [True, True],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [False, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            PandasDataset,
            [True, True],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^[1-9]{1}\d*$",
            2,
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_not_prefix_matches_regex(data, comparator, prefix, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_prefix_matches_regex({"target": "target", "comparator": comparator, "prefix": prefix})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {
                "target": ["word", "TEST"],
            },
            r".*",
            DaskDataset,
            [True, True],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            r"[0-9].*",
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            PandasDataset,
            [True, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            DaskDataset,
            [True, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^-?[1-9]{1}\d*$",
            PandasDataset,
            [True, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^-?[1-9]{1}\d*$",
            DaskDataset,
            [True, False],
        ),
    ],
)
def test_matches_regex(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.matches_regex({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,dataset_type, expected_result",
    [
        (
            {
                "target": ["word", "TEST"],
            },
            r".*",
            DaskDataset,
            [False, False],
        ),
        (
            {
                "target": ["word", "TEST"],
            },
            r"[0-9].*",
            PandasDataset,
            [True, True],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            PandasDataset,
            [False, False],
        ),
        (
            {
                "target": [224, None],
            },
            r"^[1-9]{1}\d*$",
            DaskDataset,
            [False, False],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^-?[1-9]{1}\d*$",
            PandasDataset,
            [False, True],
        ),
        (
            {
                "target": [-25, 3.14],
            },
            r"^-?[1-9]{1}\d*$",
            DaskDataset,
            [False, True],
        ),
    ],
)
def test_not_matches_regex(data, comparator, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.not_matches_regex({"target": "target", "comparator": comparator})
    assert result.equals(df.convert_to_series(expected_result))
