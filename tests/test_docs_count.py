"""Doc-drift guard for the README's stated test count.

business-correctness: the headline count in README.md must equal the live
collected count, so the documented number cannot silently drift from
reality. Adding or removing a test forces updating the README in the same
change, which is the whole point of pinning the two together.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"


def _readme_claimed_count() -> int:
    text = README.read_text(encoding="utf-8")
    match = re.search(r"contains (\d+) tests", text)
    assert match is not None, "could not find the test-count headline in README.md"
    return int(match.group(1))


def _collected_count() -> int:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    match = re.search(r"(\d+) tests collected", result.stdout)
    assert match is not None, (
        "could not parse the collected count from pytest output:\n"
        f"{result.stdout}\n{result.stderr}"
    )
    return int(match.group(1))


def test_readme_count_matches_collected_count() -> None:
    assert _readme_claimed_count() == _collected_count(), (
        "README.md test count is out of sync with the collected suite; "
        "update the README headline and per-file table to match."
    )
