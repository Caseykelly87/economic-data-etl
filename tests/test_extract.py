import json
import pytest
import requests
from datetime import datetime
from unittest.mock import MagicMock
from src import extract


# ==========================================================
# Hashing Tests
# ==========================================================

def test_compute_hash_consistency():
    data = {"a": 1}
    assert extract.compute_hash(data) == extract.compute_hash(data)


def test_compute_hash_difference():
    assert extract.compute_hash({"a": 1}) != extract.compute_hash({"a": 2})


def test_compute_hash_empty_dict():
    h = extract.compute_hash({})
    assert isinstance(h, str)
    assert len(h) == 64  # SHA256 hex digest is always 64 characters


def test_compute_hash_key_order_independent():
    """sort_keys=True means insertion order must not affect the hash."""
    assert extract.compute_hash({"b": 2, "a": 1}) == extract.compute_hash({"a": 1, "b": 2})


# ==========================================================
# Metadata Tests
# ==========================================================

def test_metadata_save_and_load(temp_dirs):
    test_metadata = {"last_hash": "abc123", "last_observation_date": "2024-01-01"}
    extract.save_metadata("FRED", "TEST", test_metadata)
    assert extract.load_metadata("FRED", "TEST") == test_metadata


def test_load_metadata_nonexistent(temp_dirs):
    assert extract.load_metadata("FRED", "DOES_NOT_EXIST") == {}


def test_metadata_file_naming(temp_dirs):
    """Metadata filename must follow the <source>_<id>_metadata.json convention."""
    _, metadata_dir = temp_dirs
    extract.save_metadata("BLS", "SERIES1", {"key": "value"})
    assert (metadata_dir / "BLS_SERIES1_metadata.json").exists()


# ==========================================================
# Storage Path Tests
# ==========================================================

def test_get_storage_path_format(temp_dirs):
    """Storage path must live in DATA_RAW_DIR and follow naming convention."""
    raw_dir, _ = temp_dirs
    path = extract.get_storage_path("FRED", "UNRATE")

    assert path.parent == raw_dir
    assert path.name.startswith("FRED_UNRATE_")
    assert path.name.endswith(".json")

    # Validate embedded date is a real date in YYYY_MM_DD format
    date_str = path.stem.replace("FRED_UNRATE_", "")
    datetime.strptime(date_str, "%Y_%m_%d")  # raises ValueError if format is wrong


# ==========================================================
# Retry Decorator Tests
# ==========================================================

def test_fetch_with_retry_succeeds_after_failures(monkeypatch):
    """Decorator should retry on RequestException and return on eventual success."""
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)

    call_count = 0

    @extract.fetch_with_retry
    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise requests.exceptions.ConnectionError("transient")
        return "ok"

    assert flaky() == "ok"
    assert call_count == 3


def test_fetch_with_retry_raises_after_max_attempts(monkeypatch):
    """After 3 failed attempts the original exception must propagate."""
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)

    @extract.fetch_with_retry
    def always_fails():
        raise requests.exceptions.ConnectionError("always down")

    with pytest.raises(requests.exceptions.ConnectionError):
        always_fails()


def test_fetch_with_retry_non_network_error_not_retried():
    """ValueError (config/logic error) must propagate immediately without retry."""
    call_count = 0

    @extract.fetch_with_retry
    def config_error():
        nonlocal call_count
        call_count += 1
        raise ValueError("bad config")

    with pytest.raises(ValueError):
        config_error()

    assert call_count == 1, "Should not retry on non-network errors"


def test_fetch_with_retry_preserves_function_name():
    """functools.wraps must preserve the decorated function's identity."""
    @extract.fetch_with_retry
    def my_func():
        pass

    assert my_func.__name__ == "my_func"


def test_fetch_with_retry_fast_fails_on_4xx(monkeypatch):
    """4xx HTTP errors must propagate immediately without retry.

    Business-correctness: client errors (bad credentials, malformed
    requests, missing endpoints) will not succeed on retry; retrying
    just wastes time and adds upstream load.
    """
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)

    call_count = 0
    response = MagicMock()
    response.status_code = 401

    @extract.fetch_with_retry
    def unauthorized():
        nonlocal call_count
        call_count += 1
        err = requests.exceptions.HTTPError("401 Unauthorized")
        err.response = response
        raise err

    with pytest.raises(requests.exceptions.HTTPError):
        unauthorized()

    assert call_count == 1, "4xx must not be retried"


def test_fetch_with_retry_applies_jitter_to_backoff(monkeypatch):
    """Backoff sleep must equal 2**attempt + the jittered offset on each retry.

    Business-correctness: asserts the jitter term is actually summed into
    the sleep duration, not just that sleep was called. Guards the backoff
    formula itself (defensive hygiene against retry-storm pile-up if this
    code is ever used in parallel), not concurrency that this single-
    threaded pipeline does not have.
    """
    sleep_calls = []
    monkeypatch.setattr(extract.time, "sleep", lambda s: sleep_calls.append(s))
    # Deterministic jitter sequence: 0.25 for attempt 0, 0.75 for attempt 1.
    jitter_values = iter([0.25, 0.75])
    monkeypatch.setattr(extract.random, "uniform", lambda a, b: next(jitter_values))

    call_count = 0

    @extract.fetch_with_retry
    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise requests.exceptions.ConnectionError("transient")
        return "ok"

    assert flaky() == "ok"
    assert sleep_calls == [1 + 0.25, 2 + 0.75]


def test_fetch_with_retry_retries_on_5xx(monkeypatch):
    """5xx HTTP errors must be retried like other transient failures."""
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)

    call_count = 0
    response = MagicMock()
    response.status_code = 503

    @extract.fetch_with_retry
    def flaky_5xx():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            err = requests.exceptions.HTTPError("503 Service Unavailable")
            err.response = response
            raise err
        return "ok"

    assert flaky_5xx() == "ok"
    assert call_count == 3


# ==========================================================
# FRED Extraction Tests
# ==========================================================

def test_fred_first_run_writes_file(temp_dirs, mock_fred_response, monkeypatch):
    raw_dir, metadata_dir = temp_dirs
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")

    mock_get = MagicMock()
    mock_get.return_value.json.return_value = mock_fred_response
    monkeypatch.setattr("requests.get", mock_get)

    extract.fetch_fred_data("TEST_SERIES")

    assert len(list(raw_dir.glob("FRED_TEST_SERIES_*.json"))) == 1
    assert (metadata_dir / "FRED_TEST_SERIES_metadata.json").exists()


def test_fred_no_change_skips_write(temp_dirs, mock_fred_response, monkeypatch):
    raw_dir, _ = temp_dirs
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")

    mock_get = MagicMock()
    mock_get.return_value.json.return_value = mock_fred_response
    monkeypatch.setattr("requests.get", mock_get)

    extract.fetch_fred_data("TEST_SERIES")
    first_files = list(raw_dir.glob("FRED_TEST_SERIES_*.json"))

    extract.fetch_fred_data("TEST_SERIES")
    second_files = list(raw_dir.glob("FRED_TEST_SERIES_*.json"))

    assert len(first_files) == 1
    assert len(second_files) == 1


def test_fred_change_creates_new_snapshot(temp_dirs, monkeypatch):
    raw_dir, _ = temp_dirs
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")

    response_v1 = {"observations": [{"date": "2024-01-01", "value": "5.0"}]}
    response_v2 = {
        "observations": [
            {"date": "2024-01-01", "value": "5.0"},
            {"date": "2024-02-01", "value": "5.2"},
        ]
    }

    mock_get = MagicMock()
    monkeypatch.setattr("requests.get", mock_get)

    mock_get.return_value.json.return_value = response_v1
    extract.fetch_fred_data("TEST_SERIES")

    mock_get.return_value.json.return_value = response_v2
    extract.fetch_fred_data("TEST_SERIES")

    files = list(raw_dir.glob("FRED_TEST_SERIES_*.json"))
    assert len(files) == 1  # Same-day overwrite, not a new file

    metadata = extract.load_metadata("FRED", "TEST_SERIES")
    assert metadata["last_observation_date"] == "2024-02-01"


def test_fred_no_api_key_raises(monkeypatch):
    """Missing FRED key must raise ValueError immediately, not after retries."""
    monkeypatch.setattr(extract, "FRED_API_KEY", None)

    with pytest.raises(ValueError, match="FRED_API_KEY not set"):
        extract.fetch_fred_data("TEST_SERIES")


def test_fred_http_error_raises(temp_dirs, monkeypatch):
    """An HTTP error from the FRED API must eventually propagate."""
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)

    mock_get = MagicMock()
    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("503")
    monkeypatch.setattr("requests.get", mock_get)

    with pytest.raises(requests.exceptions.HTTPError):
        extract.fetch_fred_data("TEST_SERIES")


def test_fred_malformed_response_raises(temp_dirs, monkeypatch):
    """A FRED response without 'observations' must raise ValueError immediately."""
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")

    mock_get = MagicMock()
    mock_get.return_value.json.return_value = {"error": "unknown series"}
    monkeypatch.setattr("requests.get", mock_get)

    with pytest.raises(ValueError, match="Malformed FRED response"):
        extract.fetch_fred_data("TEST_SERIES")


def test_fred_uses_incremental_start_date(temp_dirs, mock_fred_response, monkeypatch):
    """When prior metadata exists, observation_start should be sent to the API."""
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")

    # Pre-seed metadata with a known date and a stale hash so the run proceeds
    extract.save_metadata("FRED", "TEST_SERIES", {
        "last_observation_date": "2024-01-01",
        "last_hash": "old_hash_that_wont_match",
    })

    mock_get = MagicMock()
    mock_get.return_value.json.return_value = mock_fred_response
    monkeypatch.setattr("requests.get", mock_get)

    extract.fetch_fred_data("TEST_SERIES")

    sent_params = mock_get.call_args.kwargs["params"]
    assert sent_params.get("observation_start") == "2024-01-01"


def test_fred_empty_observations_preserves_last_date(temp_dirs, monkeypatch):
    """Empty observations list must not overwrite the stored last_observation_date."""
    monkeypatch.setattr(extract, "FRED_API_KEY", "fake_key")

    extract.save_metadata("FRED", "TEST_SERIES", {
        "last_observation_date": "2024-01-01",
        "last_hash": "stale_hash",
    })

    mock_get = MagicMock()
    mock_get.return_value.json.return_value = {"observations": []}
    monkeypatch.setattr("requests.get", mock_get)

    extract.fetch_fred_data("TEST_SERIES")

    metadata = extract.load_metadata("FRED", "TEST_SERIES")
    assert metadata["last_observation_date"] == "2024-01-01"


# ==========================================================
# BLS Extraction Tests
# ==========================================================

def test_bls_first_run_writes_file(temp_dirs, mock_bls_response, monkeypatch):
    raw_dir, _ = temp_dirs
    monkeypatch.setattr(extract, "BLS_API_KEY", "fake_key")

    mock_post = MagicMock()
    mock_post.return_value.json.return_value = mock_bls_response
    monkeypatch.setattr("requests.post", mock_post)

    extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)

    assert len(list(raw_dir.glob("BLS_batch_pull_*.json"))) == 1


def test_bls_no_change_skips_write(temp_dirs, mock_bls_response, monkeypatch):
    raw_dir, _ = temp_dirs
    monkeypatch.setattr(extract, "BLS_API_KEY", "fake_key")

    mock_post = MagicMock()
    mock_post.return_value.json.return_value = mock_bls_response
    monkeypatch.setattr("requests.post", mock_post)

    extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)
    extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)

    assert len(list(raw_dir.glob("BLS_batch_pull_*.json"))) == 1


def test_bls_change_creates_new_snapshot(temp_dirs, monkeypatch):
    """Updated BLS data should overwrite the same-day file and store the new content."""
    raw_dir, _ = temp_dirs
    monkeypatch.setattr(extract, "BLS_API_KEY", "fake_key")

    response_v1 = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {"series": [{"seriesID": "TEST123", "data": [{"year": "2024", "period": "M01", "value": "100"}]}]},
    }
    response_v2 = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {"series": [{"seriesID": "TEST123", "data": [{"year": "2024", "period": "M02", "value": "101"}]}]},
    }

    mock_post = MagicMock()
    monkeypatch.setattr("requests.post", mock_post)

    mock_post.return_value.json.return_value = response_v1
    extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)

    mock_post.return_value.json.return_value = response_v2
    extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)

    files = list(raw_dir.glob("BLS_batch_pull_*.json"))
    assert len(files) == 1

    with open(files[0]) as f:
        assert json.load(f) == response_v2


def test_bls_no_api_key_raises(monkeypatch):
    """Missing BLS key must raise ValueError immediately, not after retries."""
    monkeypatch.setattr(extract, "BLS_API_KEY", None)

    with pytest.raises(ValueError, match="BLS_API_KEY not set"):
        extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)


def test_bls_api_error_status_raises(temp_dirs, monkeypatch):
    """A REQUEST_FAILED status from the BLS API must raise RuntimeError."""
    monkeypatch.setattr(extract, "BLS_API_KEY", "fake_key")

    mock_post = MagicMock()
    mock_post.return_value.json.return_value = {
        "status": "REQUEST_FAILED",
        "message": ["Invalid series ID"],
    }
    monkeypatch.setattr("requests.post", mock_post)

    with pytest.raises(RuntimeError, match="BLS API Error"):
        extract.fetch_bls_data({"TEST": "INVALID"}, 2024, 2024)


def test_bls_http_error_raises(temp_dirs, monkeypatch):
    """An HTTP error from the BLS API must eventually propagate."""
    monkeypatch.setattr(extract, "BLS_API_KEY", "fake_key")
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)

    mock_post = MagicMock()
    mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
    monkeypatch.setattr("requests.post", mock_post)

    with pytest.raises(requests.exceptions.HTTPError):
        extract.fetch_bls_data({"TEST": "TEST123"}, 2024, 2024)


# ==========================================================
# ERS URL Discovery Tests
# ==========================================================


def test_get_dynamic_ers_url_falls_back_on_failure(monkeypatch):
    """When discovery raises an exception, the fallback URL is returned."""
    monkeypatch.setattr("requests.get", MagicMock(side_effect=Exception("timeout")))

    url = extract.get_dynamic_ers_url()

    assert url == extract._ERS_FALLBACK_URL


# ==========================================================
# ERS Price Outlook Extraction Tests
# ==========================================================

def test_ers_first_run_writes_file(temp_dirs, monkeypatch):
    raw_dir, metadata_dir = temp_dirs

    csv_text = "Category,Year,Forecast_Midpoint\nAll food,2024,2.1\nFood at home,2024,1.8\n"
    mock_get = MagicMock()
    mock_get.return_value.text = csv_text
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr(extract, "get_dynamic_ers_url", lambda: "https://fake.url/test.csv")

    extract.fetch_ers_price_outlook()

    assert len(list(raw_dir.glob("ERS_cpi_forecasts_*.json"))) == 1
    assert (metadata_dir / "ERS_cpi_forecasts_metadata.json").exists()


def test_ers_no_change_skips_write(temp_dirs, monkeypatch):
    raw_dir, _ = temp_dirs

    csv_text = "Category,Year,Forecast_Midpoint\nAll food,2024,2.1\n"
    mock_get = MagicMock()
    mock_get.return_value.text = csv_text
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr(extract, "get_dynamic_ers_url", lambda: "https://fake.url/test.csv")

    extract.fetch_ers_price_outlook()
    extract.fetch_ers_price_outlook()

    assert len(list(raw_dir.glob("ERS_cpi_forecasts_*.json"))) == 1


def test_ers_http_error_raises(temp_dirs, monkeypatch):
    monkeypatch.setattr(extract.time, "sleep", lambda _: None)
    monkeypatch.setattr(extract, "get_dynamic_ers_url", lambda: "https://fake.url/test.csv")

    mock_get = MagicMock()
    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("403")
    monkeypatch.setattr("requests.get", mock_get)

    with pytest.raises(requests.exceptions.HTTPError):
        extract.fetch_ers_price_outlook()


