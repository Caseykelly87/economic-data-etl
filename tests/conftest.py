import pytest
from src import extract


@pytest.fixture
def temp_dirs(tmp_path, monkeypatch):
    """
    Redirect raw and metadata directories to pytest temp dir so tests
    never touch real project data folders.
    """
    raw_dir = tmp_path / "raw"
    metadata_dir = tmp_path / "metadata"

    raw_dir.mkdir()
    metadata_dir.mkdir()

    monkeypatch.setattr(extract, "DATA_RAW_DIR", raw_dir)
    monkeypatch.setattr(extract, "DATA_METADATA_DIR", metadata_dir)

    return raw_dir, metadata_dir


@pytest.fixture
def mock_fred_response():
    """Minimal valid FRED API response."""
    return {
        "observations": [
            {"date": "2024-01-01", "value": "5.0"},
            {"date": "2024-02-01", "value": "5.1"},
        ]
    }


@pytest.fixture
def mock_bls_response():
    """Minimal valid BLS API response."""
    return {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "TEST123",
                    "data": [{"year": "2024", "period": "M01", "value": "100"}],
                }
            ]
        },
    }
