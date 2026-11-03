"""Runs on Databricks (pytest -m integration) via the CLI: 3 districts x 7 days through ingest -> bronze, twice (idempotency)."""

import json
import os
import subprocess
import time

import pytest

pytestmark = pytest.mark.integration
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE")  # unset in CI: host/token come from env vars


def cli(*args) -> str:
    cmd = ["databricks", *args] + (["--profile", PROFILE] if PROFILE else [])
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


def run_ingest(params: str) -> dict:
    cli("bundle", "run", "agripulse_ingest", "-t", "dev", "--params", params)
    run = json.loads(cli("jobs", "list-runs", "--limit", "1", "-o", "json"))[0]
    assert run["state"]["result_state"] == "SUCCESS", run["state"]
    full = json.loads(cli("jobs", "get-run", str(run["run_id"]), "-o", "json"))
    out = json.loads(cli("jobs", "get-run-output", str(full["tasks"][0]["run_id"]), "-o", "json"))
    return json.loads(out["notebook_output"]["result"])


def test_three_districts_seven_days_is_idempotent():
    params = "source=nasa_power,mode=backfill,start=2024-01-01,end=2024-01-07,n_districts=3"
    first = run_ingest(params)
    time.sleep(1)
    second = run_ingest(params)
    assert first["rows_in"] == 21
    assert second["inserted"] == 0  # re-running the same window adds no rows
