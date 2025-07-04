from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset


def test_from_dict(db_config):
    dict = {"test": ["A", "B", "C"]}

    dataset = SQLiteDataset.from_dict(dict, db_config)
    assert all([list(row.keys())[0] == "test" for row in dataset.data])


def test_from_records(db_config):
    records = [
        {"col1": 1, "col2": "A", "col3": 4},
        {"col1": 2, "col2": "B", "col3": 5},
        {"col1": 3, "col2": "C", "col3": 6},
    ]

    dataset = SQLiteDataset.from_records(records, db_config)

    stored_records = list(dataset.iterrows())
    assert len(stored_records) == 3

    for i, (_, row_data) in enumerate(stored_records):
        assert row_data["col1"] == records[i]["col1"]
        assert row_data["col2"] == records[i]["col2"]
        assert row_data["col3"] == records[i]["col3"]


def test_getitem_single_column(db_config):
    data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    col1_data = dataset["col1"]
    assert col1_data == [1, 2, 3]
    
    col2_data = dataset["col2"]
    assert col2_data == ["A", "B", "C"]


def test_getitem_multiple_columns(db_config):
    data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"], "col3": [True, False, True]}
    dataset = SQLiteDataset.from_dict(data, db_config)

    subset = dataset[["col1", "col3"]]
    assert "col1" in subset.columns
    assert "col3" in subset.columns
    assert "col2" not in subset.columns
    assert len(subset) == 3


def test_columns_property(db_config):
    data = {"col1": [1, 2], "col2": ["A", "B"], "col3": [True, False]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    columns = dataset.columns
    assert len(columns) == 3
    assert "col1" in columns
    assert "col2" in columns
    assert "col3" in columns


def test_len(db_config):
    empty_dataset = SQLiteDataset.from_dict({}, db_config)
    assert len(empty_dataset) == 0
    
    data = {"col1": [1, 2, 3, 4, 5]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    assert len(dataset) == 5


def test_merge_inner(db_config):
    left_data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    right_data = {"col1": [2, 3, 4], "col3": [100, 200, 300]}
    
    left = SQLiteDataset.from_dict(left_data, db_config)
    right = SQLiteDataset.from_dict(right_data, db_config)
    
    merged = left.merge(right, on="col1", how="inner")
    
    assert len(merged) == 2 # only two columns match
    assert "col2" in merged.columns
    assert "col3" in merged.columns
    
    records = list(merged.iterrows())
    assert any(row["col1"] == 2 and row["col2"] == "B" and row["col3"] == 100 for _, row in records)
    assert any(row["col1"] == 3 and row["col2"] == "C" and row["col3"] == 200 for _, row in records)


def test_merge_left(db_config):
    left_data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    right_data = {"col1": [2, 3, 4], "col3": [85, 90, 95]}
    
    left = SQLiteDataset.from_dict(left_data, db_config)
    right = SQLiteDataset.from_dict(right_data, db_config)

    merged = left.merge(right, on="col1", how="left")
    # currently returning merged.data as [{'col1': 1, 'col2': 'A'}, {'col1': 2, 'col2': 'B', 'col3': 85}, {'col1': 3, 'col2': 'C', 'col3': 90}]
    
    assert len(merged) == 3  # all left records preserved
    records = list(merged.iterrows())
    
    col1_1_record = next((row for _, row in records if row["col1"] == 1), None)
    assert col1_1_record is not None
    assert col1_1_record["col2"] == "A"
    assert col1_1_record["col3"] is None


def test_empty_property(db_config):
    empty_dataset = SQLiteDataset.from_dict({}, db_config)
    assert empty_dataset.empty is True

    data = {"col1": [1, 2, 3]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    assert dataset.empty is False


def test_iterrows(db_config):
    data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    rows = list(dataset.iterrows())
    assert len(rows) == 3
    
    idx, row_data = rows[0]
    assert idx == 0
    assert row_data["col1"] == 1
    assert row_data["col2"] == "A"
    
    indices = [idx for idx, _ in rows]
    assert indices == [0, 1, 2]


def test_groupby_single_column(db_config):
    data = {
        "col1": ["A", "B", "A", "B", "A"],
        "col2": [10, 20, 15, 25, 30]
    }
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    grouped = dataset.groupby("col1")
    result = grouped.agg({"col2": "sum"})
    
    records = {row["col1"]: row["col2_sum"] for _, row in result.iterrows()}
    assert records["A"] == 55  # 10 + 15 + 30
    assert records["B"] == 45  # 20 + 25


def test_groupby_multiple_columns(db_config):
    data = {
        "col1": ["A", "A", "B", "B"],
        "col2": [1, 2, 1, 1],
        "col3": [100, 150, 80, 120]
    }
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    grouped = dataset.groupby(["col1", "col2"])
    result = grouped.agg({"col3": "mean"})
    
    assert len(result) == 3


def test_get_with_default(db_config):
    data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    col1 = dataset.get("col1")
    assert col1 == [1, 2, 3]
    
    col3 = dataset.get("col3", default=[])
    assert col3 == []
    
    col4 = dataset.get("col4")
    assert col4 is None


def test_concat_vertical(db_config):
    data1 = {"col1": [1, 2], "col2": ["A", "B"]}
    data2 = {"col1": [3, 4], "col2": ["C", "D"]}
    
    ds1 = SQLiteDataset.from_dict(data1, db_config)
    ds2 = SQLiteDataset.from_dict(data2, db_config)

    result = ds1.concat(ds2)
    assert len(result) == 4
    
    col1s = result["col1"]
    assert col1s == [1, 2, 3, 4]
    
    data3 = {"col1": [5], "col2": ["E"]}
    ds3 = SQLiteDataset.from_dict(data3, db_config)
    
    result2 = ds1.concat([ds2, ds3])
    assert len(result2) == 5


def test_concat_horizontal(db_config):
    data1 = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    data2 = {"col3": [4, 5, 6], "col4": ["D", "E", "F"]}
    
    ds1 = SQLiteDataset.from_dict(data1, db_config)
    ds2 = SQLiteDataset.from_dict(data2, db_config)
    
    result = ds1.concat(ds2, axis=1)
    
    assert len(result) == 3
    assert "col1" in result.columns
    assert "col2" in result.columns
    assert "col3" in result.columns
    assert "col4" in result.columns


def test_rename_columns(db_config):
    data = {"old_col1": [1, 2, 3], "old_col2": ["A", "B", "C"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    renamed = dataset.rename(columns={"old_col1": "new_col1", "old_col2": "new_col2"})
    
    assert "new_col1" in renamed.columns
    assert "new_col2" in renamed.columns
    assert "old_col1" not in renamed.columns
    assert "old_col2" not in renamed.columns
    
    assert renamed["new_col1"] == [1, 2, 3]
    assert renamed["new_col2"] == ["A", "B", "C"]


def test_drop_columns(db_config):
    data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"], "col3": [True, False, True]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    dropped = dataset.drop(columns=["col2"])
    
    assert "col1" in dropped.columns
    assert "col2" not in dropped.columns
    assert "col3" in dropped.columns
    assert len(dropped) == 3
    
    dropped2 = dataset.drop(columns=["col1", "col3"])
    assert "col2" in dropped2.columns
    assert len(dropped2.columns) == 1


def test_unique(db_config):
    data = {"col1": [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    unique_vals = dataset.unique("col1")
    assert sorted(unique_vals) == [1, 2, 3, 4]

    data_with_none = {"col1": [1, 2, None, 2, None, 3]}
    dataset2 = SQLiteDataset.from_dict(data_with_none, db_config)
    unique_vals2 = dataset2.unique("col1")
    assert set(unique_vals2) == {1, 2, 3}


def test_fillna(db_config):
    data = {"col1": [1, None, 3], "col2": ["A", None, "C"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    filled = dataset.fillna(value=0)
    
    col1_data = filled["col1"]
    col2_data = filled["col2"]
    
    assert col1_data == [1, 0, 3]
    assert col2_data == ["A", 0, "C"]

    data_ffill = {"col1": [1, None, None, 4, None]}
    dataset_ffill = SQLiteDataset.from_dict(data_ffill, db_config)
    filled_ffill = dataset_ffill.fillna(method="ffill")
    
    assert filled_ffill["col1"] == [1, 1, 1, 4, 4]


def test_sort_values_single_column(db_config):
    data = {"col1": [3, 1, 4, 2], "col2": ["C", "A", "D", "B"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    sorted_asc = dataset.sort_values("col1")
    assert sorted_asc["col1"] == [1, 2, 3, 4]
    assert sorted_asc["col2"] == ["A", "B", "C", "D"]
    
    sorted_desc = dataset.sort_values("col2", ascending=False)
    assert sorted_desc["col2"] == ["D", "C", "B", "A"]


def test_sort_values_multiple_columns(db_config):
    data = {
        "col1": ["A", "B", "A", "B"],
        "col2": [1, 2, 3, 3],
        "col3": ["C", "D", "E", "F"]
    }
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    sorted_ds = dataset.sort_values(["col1", "col2"])

    col3s = sorted_ds["col3"]
    assert col3s == ["C", "E", "D", "F"]


def test_reset_index(db_config):
    data = {"col1": [1, 2, 3]}
    dataset = SQLiteDataset.from_dict(data, db_config)

    reset = dataset.reset_index()
    
    for i, (idx, _) in enumerate(reset.iterrows()):
        assert idx == i
    
    reset_keep = dataset.reset_index(drop=False)
    assert "index" in reset_keep.columns


def test_iloc_single_value(db_config):
    data = {"col1": [10, 20, 30], "col2": ["A", "B", "C"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    # dataset.data returns [{'col1': 10, 'col2': 'A'}, {'col1': 20, 'col2': 'B'}, {'col1': 30, 'col2': 'C'}]
    
    value = dataset.iloc(0, 0)
    assert value == 10
    
    value = dataset.iloc(1, 1)
    assert value == "B"


def test_iloc_row_slice(db_config):
    data = {"col1": [1, 2, 3, 4, 5], "col2": ["A", "B", "C", "D", "E"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    # returns [{'col1': 1, 'col2': 'A'}, {'col1': 2, 'col2': 'B'}, {'col1': 3, 'col2': 'C'}, {'col1': 4, 'col2': 'D'}, {'col1': 5, 'col2': 'E'}]
    
    subset = dataset.iloc(slice(1, 4))
    assert len(subset) == 3
    assert subset["col1"] == [2, 3, 4]
    assert subset["col2"] == ["B", "C", "D"]


def test_astype(db_config):
    data = {"int_col": [1, 2, 3], "str_col": ["10", "20", "30"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    converted = dataset.astype({"str_col": int})
    
    str_col_data = converted["str_col"]
    assert str_col_data == [10, 20, 30]
    assert all(isinstance(x, int) for x in str_col_data)
    
    converted2 = dataset.astype({"int_col": str})
    int_col_data = converted2["int_col"]
    assert int_col_data == ["1", "2", "3"]
    assert all(isinstance(x, str) for x in int_col_data)


def test_apply_row_wise(db_config):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    def row_sum(row):
        return row["a"] + row["b"]
    
    result = dataset.apply(row_sum, axis=1)
    
    sums = [row["result"] for _, row in result.iterrows()]
    assert sums == [5, 7, 9]


def test_apply_column_wise(db_config):
    data = {"col1": [1, 2, 3, 4], "col2": [10, 20, 30, 40]}
    dataset = SQLiteDataset.from_dict(data, db_config)

    def col_mean(values):
        return sum(values) / len(values)
    
    result = dataset.apply(col_mean, axis=0)
    
    assert result["col1"] == 2.5
    assert result["col2"] == 25.0


# Edge cases and additional tests

def test_empty_dataset_operations(db_config):
    empty = SQLiteDataset.from_dict({}, db_config)
    
    assert empty.empty is True
    assert len(empty) == 0
    assert list(empty.iterrows()) == []
    
    data = {"col1": [1, 2], "col2": ["A", "B"]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    merged = dataset.merge(empty, on="col1", how="left")
    assert len(merged) == 2


def test_none_handling(db_config):
    data = {"col1": [1, None, 3, None, 5], "col2": [None, "B", None, "D", None]}
    dataset = SQLiteDataset.from_dict(data, db_config)
    
    col1_data = dataset["col1"]
    assert col1_data[1] is None
    assert col1_data[3] is None

    unique_col1 = dataset.unique("col1")
    assert set(unique_col1) == {1, 3, 5}