from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset


def test_from_dict():
    data = {"test": ["A", "B", "C"]}

    dataset = SQLiteDataset.from_dict(data)
    assert "test" in dataset.data
