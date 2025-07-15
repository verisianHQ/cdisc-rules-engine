from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset


def test_from_dict(dataset_kwargs):
    """Test creating a SQLiteDataset from a dictionary."""
    dict = {"test": ["A", "B", "C"]}  # dummy data
    dataset = SQLiteDataset.from_dict(dict, **dataset_kwargs)
    assert [row[list(dict.keys())[0]] for row in dataset.data] == list(dict.values())[0]


def test_from_records(dataset_kwargs):
    """Test creating a SQLiteDataset from a list of records."""
    records = [
        {"col1": 1, "col2": "A", "col3": 4},
        {"col1": 2, "col2": "B", "col3": 5},
        {"col1": 3, "col2": "C", "col3": 6},
    ]  # dummy data

    dataset = SQLiteDataset.from_records(records, **dataset_kwargs)

    for i, row_data in enumerate(dataset.data):
        for col in records[0].keys():
            assert row_data[col] == records[i][col]
