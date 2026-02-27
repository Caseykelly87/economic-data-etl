import logging
import pytest
from unittest.mock import patch, call
from src import main
from src.config import FRED_SERIES, BLS_SERIES


# ==========================================================
# Pipeline Orchestration Tests
# ==========================================================

def test_run_pipeline_calls_fred_for_every_series():
    """run_pipeline must call fetch_fred_data once per configured FRED series."""
    with patch("src.main.fetch_fred_data") as mock_fred, \
         patch("src.main.fetch_bls_data"):

        main.run_pipeline()

        assert mock_fred.call_count == len(FRED_SERIES)
        expected_calls = [call(sid) for sid in FRED_SERIES.values()]
        mock_fred.assert_has_calls(expected_calls, any_order=False)


def test_run_pipeline_calls_bls_with_correct_args():
    """run_pipeline must call fetch_bls_data with BLS_SERIES and the configured year range."""
    with patch("src.main.fetch_fred_data"), \
         patch("src.main.fetch_bls_data") as mock_bls:

        main.run_pipeline()

        mock_bls.assert_called_once_with(BLS_SERIES, 2021, 2026)


def test_run_pipeline_handles_fred_error_gracefully(caplog):
    """An extraction error must be logged and must not raise out of run_pipeline."""
    with patch("src.main.fetch_fred_data", side_effect=Exception("API down")), \
         patch("src.main.fetch_bls_data") as mock_bls:

        with caplog.at_level(logging.ERROR):
            main.run_pipeline()  # Must not raise

        assert "Pipeline failed" in caplog.text
        mock_bls.assert_not_called()  # BLS should not run when FRED extraction fails


def test_run_pipeline_handles_bls_error_gracefully(caplog):
    """A BLS extraction error must be logged and must not raise out of run_pipeline."""
    with patch("src.main.fetch_fred_data"), \
         patch("src.main.fetch_bls_data", side_effect=RuntimeError("BLS down")):

        with caplog.at_level(logging.ERROR):
            main.run_pipeline()  # Must not raise

        assert "Pipeline failed" in caplog.text
