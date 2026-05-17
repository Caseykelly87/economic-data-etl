"""Tests for the structlog configurator."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest
import structlog

from src.observability import configure_logging


@pytest.fixture(autouse=True)
def reset_structlog():
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


class TestConfigureLogging:
    def test_default_invocation_runs_without_error(self):
        configure_logging()

    def test_json_format_emits_parseable_json(self):
        with patch.dict(os.environ, {"LOG_FORMAT": "json"}, clear=False):
            configure_logging()
            buf = io.StringIO()
            with redirect_stdout(buf):
                logger = structlog.get_logger("test")
                logger.info("test_event", row_count=42, source="fred")
            line = buf.getvalue().strip().split("\n")[-1]
            assert line, "expected at least one line of json output"
            payload = json.loads(line)
            assert payload["event"] == "test_event"
            assert payload["row_count"] == 42
            assert payload["source"] == "fred"
            assert "timestamp" in payload
            assert payload["level"] == "info"

    def test_log_level_env_var_filters_below_threshold(self):
        with patch.dict(os.environ, {"LOG_LEVEL": "warning", "LOG_FORMAT": "json"}, clear=False):
            configure_logging()
            buf = io.StringIO()
            with redirect_stdout(buf):
                logger = structlog.get_logger("test")
                logger.info("info_message")
                logger.warning("warn_message")
            output = buf.getvalue()
            assert "info_message" not in output
            assert "warn_message" in output
