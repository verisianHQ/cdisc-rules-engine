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
