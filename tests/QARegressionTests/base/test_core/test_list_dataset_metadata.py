import json
import os
import unittest
from click.testing import CliRunner

from core import list_dataset_metadata
from QARegressionTests.base.test_core.test_utils import tearDown


class TestListDatasetMetadata(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_dataset_metadata_with_valid_paths(self):
        result = self.runner.invoke(
            list_dataset_metadata,
            ["-dp", os.path.join("tests", "resources", "test_dataset.xpt")],
        )
        expected_output = """[
    {
        "domain": "EX",
        "filename": "test_dataset.xpt","""
        self.assertEqual(result.exit_code, 0)
        self.assertIn(expected_output, result.output)

    def test_list_dataset_metadata_with_invalid_paths(self):
        result = self.runner.invoke(list_dataset_metadata, ["-dp", "invalid_path"])
        expected_output = """[
    {
        "domain": "EX",
        "file_size": 823120,
        "label": "Exposure",
    """
        self.assertEqual(result.exit_code, 1)
        self.assertNotIn(expected_output, result.output)

    def test_list_dataset_metadata_sql_engine_formats(self):
        result = self.runner.invoke(
            list_dataset_metadata,
            [
                "-dp",
                os.path.join("tests", "resources", "test_dataset.xpt"),
                "-dp",
                os.path.join("tests", "resources", "clinical_data", "sas7bdat", "sdtm", "ae.sas7bdat"),
                "-dp",
                os.path.join("tests", "resources", "clinical_data", "sas7bdat", "adam", "adsl.sas7bdat"),
            ],
        )
        self.assertEqual(result.exit_code, 0)
        metadata = json.loads(result.output[result.output.index("[") :])
        self.assertEqual(
            [(item["domain"], item["filename"], item["label"]) for item in metadata],
            [
                ("EX", "test_dataset.xpt", "Exposure"),
                ("AE", "ae.sas7bdat", ""),
                (None, "adsl.sas7bdat", ""),
            ],
        )
        self.assertEqual(metadata[0]["file_size"], 823120)

    def test_list_dataset_metadata_cdisc_engine(self):
        result = self.runner.invoke(
            list_dataset_metadata,
            ["--cdisc", "-dp", os.path.join("tests", "resources", "test_dataset.xpt")],
        )
        expected_output = """[
    {
        "domain": "EX",
        "filename": "test_dataset.xpt","""
        self.assertEqual(result.exit_code, 0)
        self.assertIn(expected_output, result.output)

    def test_list_dataset_metadata_with_no_paths(self):
        result = self.runner.invoke(list_dataset_metadata)
        expected_output = """Error: Missing option '-dp' / '--dataset-path'"""
        self.assertEqual(result.exit_code, 2)
        self.assertIn(expected_output, result.output)

    def tearDown(self):
        tearDown()


if __name__ == "__main__":
    unittest.main()
