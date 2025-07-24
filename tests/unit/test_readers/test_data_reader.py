import pytest
from cdisc_rules_engine.readers.data_reader import DataReader


ADAM_DOMAINS = ["ADAE", "ADEF", "ADSL", "ADTTE"]
SDTM_DOMAINS = ["AE", "DM", "EX", "LB", "SUPPDM", "TA", "TD", "TE", "TI", "TS", "TV", "XP"]


@pytest.fixture
def clinical_data_directory(resources_directory):
    """Get the clinical data directory."""
    return resources_directory / "clinical_data"


@pytest.fixture
def metadata_directory(resources_directory):
    """Get the metadata directory."""
    return resources_directory / "clinical_data" / "variable_metadata"


def get_all_data_files(clinical_data_directory):
    """Get all clinical data files."""
    xpt_files = list((clinical_data_directory / "xpt").glob("*.xpt"))
    sas_files = list((clinical_data_directory / "sas7bdat").glob("*.sas7bdat"))
    return sorted(xpt_files + sas_files)


def test_metadata_extraction(clinical_data_directory, metadata_directory):
    """Test metadata extraction for SDTM files."""
    files = get_all_data_files(clinical_data_directory)

    for data_file in files:
        if not data_file.exists():
            pytest.skip("Test file not found")

        reader = DataReader(str(data_file), str(metadata_directory))

        assert reader.metadata.domain in ADAM_DOMAINS + SDTM_DOMAINS
        assert reader.metadata.standard_type in ("SDTM", "ADaM")
        assert reader.metadata.file_format in ("xpt", "sas7bdat")


def test_all_files_readable(clinical_data_directory, metadata_directory):
    """Test that all clinical data files can be read."""
    files = get_all_data_files(clinical_data_directory)

    assert len(files) > 0, "No clinical data files found"

    for file_path in files:
        try:
            reader = DataReader(str(file_path), str(metadata_directory))
            result = reader.read()

            assert isinstance(result, dict)
            assert "metadata" in result
            assert "data" in result
            assert "variables" in result
            assert len(result["data"]) > 0, f"No data in {file_path.name}"

        except Exception as e:
            pytest.fail(f"Failed to read {file_path.name}: {str(e)}")


def test_standard_type_classification(clinical_data_directory, metadata_directory):
    """Test that files are correctly classified as ADaM or SDTM."""
    files = get_all_data_files(clinical_data_directory)

    assert len(files) > 0, "No clinical data files found"

    for file_path in files:
        reader = DataReader(str(file_path), str(metadata_directory))
        domain = reader.metadata.domain

        if domain in ADAM_DOMAINS:
            assert reader.metadata.standard_type == "ADaM", f"{domain} should be classified as ADaM"
        elif domain in SDTM_DOMAINS:
            assert reader.metadata.standard_type == "SDTM", f"{domain} should be classified as SDTM"
        else:
            raise ValueError(f"Unknown domain {domain} in file {file_path.name}")


def test_file_format_support(clinical_data_directory, metadata_directory):
    """Test reading both XPT and SAS7BDAT formats."""
    files = get_all_data_files(clinical_data_directory)

    for data_file in files:
        if data_file.suffix == ".xpt":
            if data_file.exists():
                reader = DataReader(str(data_file), str(metadata_directory))
                result = reader.read()
                assert reader.metadata.file_format == "xpt"
                assert len(result["data"]) > 0
        elif data_file.suffix == ".sas7bdat":
            if data_file.exists():
                reader = DataReader(str(data_file), str(metadata_directory))
                result = reader.read()
                assert reader.metadata.file_format == "sas7bdat"
                assert len(result["data"]) > 0
        else:
            raise ValueError(f"Unsupported file format for {data_file.name}")


def test_with_variable_metadata(clinical_data_directory, metadata_directory):
    """Test reading with variable metadata."""
    files = get_all_data_files(clinical_data_directory)

    for data_file in files:
        if not data_file.exists():
            pytest.skip("Required files not found")

        reader = DataReader(str(data_file), str(metadata_directory))
        result = reader.read()

        assert "expected_variables" in result
        assert "missing_variables" in result
        assert "unexpected_variables" in result
