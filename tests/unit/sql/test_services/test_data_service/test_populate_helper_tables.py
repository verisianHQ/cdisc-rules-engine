from cdisc_rules_engine.data_service.startup.populate_helper_tables import _normalise_helper_table_data


def test_normalise_helper_table_data_converts_column_lists_to_rows():
    data = {"fda_guides": ["guide1", "guide2"]}

    result = _normalise_helper_table_data(data)

    assert result == [{"fda_guides": "guide1"}, {"fda_guides": "guide2"}]


def test_normalise_helper_table_data_keeps_non_list_dict_shape():
    data = {"fda_guides": "guide1"}

    result = _normalise_helper_table_data(data)

    assert result == data


def test_normalise_helper_table_data_keeps_uneven_column_lengths():
    data = {"col1": ["a", "b"], "col2": [1]}

    result = _normalise_helper_table_data(data)

    assert result == data
