import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from agripulse.common.config import get_settings
from agripulse.ingest import base as base_mod
from agripulse.ingest.agmarknet import AgmarknetIngestor
from agripulse.ingest.base import BaseIngestor
from agripulse.ingest.nasa_power import NasaPowerIngestor
from agripulse.ingest.openmeteo import OpenMeteoArchiveIngestor, OpenMeteoForecastIngestor
from agripulse.quality import catalog_export, freshness, scorecard
from agripulse.transform import gold_docs
from helpers import FakeSpark  # noqa: E402

FX = Path(__file__).parent.parent / "fixtures"
S = get_settings()


# ----------------------------------------------------------------------------------------------- freshness.run_checks
def _fresh_spark(weather_max=date(2026, 11, 1), pass_rate=0.99, runs=(1000,) * 8):
    handlers = [
        (r"weather_daily WHERE NOT is_forecast", [{"m": weather_max}]),
        (r"agro_climate_daily", [{"m": date(2026, 10, 30)}]),
        (r"mandi_prices_daily", [{"m": date(2017, 6, 22)}]),
        (r"weather_forecast_raw", [{"m": date(2026, 11, 4)}]),
        (r"dq_scorecard", [{"table": "mandi_prices_daily", "pass_rate": pass_rate}]),
        (r"pipeline_runs", [{"rows_in": r} for r in runs]),
    ]
    tables = {S.table("ops", "freshness_sla"): [
        {"source": "weather", "expected_lag_days": 3}, {"source": "nasa_power", "expected_lag_days": 5},
        {"source": "forecast", "expected_lag_days": 1}, {"source": "mandi", "expected_lag_days": None},
    ]}  # fmt: skip
    return FakeSpark(handlers, tables)


def test_run_checks_healthy_has_no_alerts():
    sp = _fresh_spark()
    res = freshness.run_checks(sp, S, today=date(2026, 11, 4), send=False)
    assert res["alerts"] == [] and res["checked"] == 4
    names = [n for n, *_ in sp.saved]
    assert any(n.endswith("freshness_status") for n in names)


def test_run_checks_raises_each_alert_kind():
    sp = _fresh_spark(pass_rate=0.90, runs=(100, 1000, 1000, 1000, 1000))
    res = freshness.run_checks(sp, S, today=date(2026, 11, 4), sla_override={"weather": -1}, send=False)
    kinds = {k for k, *_ in res["alerts"]}
    assert kinds == {"freshness", "dq", "volume"}
    assert any(n.endswith("alert_log") for n, *_ in sp.saved)


def test_run_checks_delivers_to_slack_and_marks_delivered(monkeypatch):
    sp = _fresh_spark(pass_rate=0.5)
    sent = {}
    monkeypatch.setattr(freshness, "get_secret", lambda *_a, **_k: "https://hooks.example/x")
    monkeypatch.setattr(freshness, "post_webhook", lambda url, alerts: sent.setdefault("n", len(alerts)) and True)
    freshness.run_checks(sp, S, today=date(2026, 11, 4), send=True)
    assert sent["n"] == 1 and any("SET delivered = true" in q for q in sp.sql_log)


def test_run_checks_survives_missing_webhook(monkeypatch):
    sp = _fresh_spark(pass_rate=0.5)

    def boom(*_a, **_k):
        raise RuntimeError("no secret")

    monkeypatch.setattr(freshness, "get_secret", boom)
    res = freshness.run_checks(sp, S, today=date(2026, 11, 4), send=True)
    assert len(res["alerts"]) == 1 and not any("SET delivered = true" in q for q in sp.sql_log)


# ----------------------------------------------------------------------------------------------- catalog + docs
CFG = {
    "owner": "kd",
    "tables": {"silver.t": {"layer": "silver", "source": "src", "description": "A table", "refresh": "daily (x)", "sla": "1d",
                             "grain": "row", "keys": ["a"], "source_url": "http://u", "licence": "L", "known_issues": "none"}},
    "columns": {"a": "col a", "b": "col b"},
}  # fmt: skip


def test_apply_metadata_comments_and_tags():
    sp = FakeSpark(
        [(r"information_schema.columns", [{"column_name": "a"}, {"column_name": "b"}, {"column_name": "__hidden"}])],
        existing=["agripulse.silver.t"],
    )
    res = catalog_export.apply_metadata(sp, CFG)
    assert res["column_comments_applied"] == 2 and res["problems"] == []
    assert any("COMMENT ON TABLE agripulse.silver.t" in q for q in sp.sql_log)
    assert any("SET TAGS" in q and "'pii' = 'false'" in q for q in sp.sql_log)


def test_apply_metadata_reports_undocumented_column():
    sp = FakeSpark([(r"information_schema.columns", [{"column_name": "zzz"}])], existing=["agripulse.silver.t"])
    assert "silver.t.zzz" in catalog_export.apply_metadata(sp, CFG)["problems"][0]


def test_missing_comments_ignores_system_tables():
    sp = FakeSpark([
        (r"information_schema.tables", [{"table_schema": "silver", "table_name": "x", "comment": None},
                                        {"table_schema": "silver", "table_name": "__materialization_1", "comment": None}]),
        (r"information_schema.columns", [{"table_schema": "silver", "table_name": "x", "column_name": "c", "comment": ""},
                                         {"table_schema": "silver", "table_name": "x", "column_name": "__enzyme", "comment": None}]),
    ])  # fmt: skip
    assert catalog_export.missing_comments(sp) == ["silver.x (table)", "silver.x.c"]


def test_render_markdown_and_tag_value():
    sp = FakeSpark([(r"information_schema.columns", [{"column_name": "a", "data_type": "STRING", "comment": "col a"}])], existing=["agripulse.silver.t"])
    md = catalog_export.render_markdown(sp, CFG)
    assert "### `agripulse.silver.t`" in md and "| `a` | STRING | col a |" in md and "Known issues" in md
    assert catalog_export._tag_value("daily (catch-up job)") == "daily"


def test_gold_docs_cover_every_feature_and_apply():
    md = gold_docs.render_markdown()
    assert all(f"`{c}`" in md for c, *_ in gold_docs.FEATURES) and "Leakage rules" in md
    sp = FakeSpark()
    gold_docs.apply_comments(sp)
    assert len(sp.sql_log) == len(gold_docs.FEATURES) + 1


def test_scorecard_pass_rate():
    assert scorecard.pass_rate(90, 10) == 0.9 and scorecard.pass_rate(0, 0) == 1.0


# ----------------------------------------------------------------------------------------------- ingestion framework
class DummyIngestor(BaseIngestor):
    source, table, keys = "dummy", "dummy_raw", ["k"]
    lag_days = 0
    default_start = date(2024, 1, 1)

    def __init__(self, *a, rows=None, fail=False, **k):
        super().__init__(*a, **k)
        self.rows, self.fail, self.loaded = rows or [], fail, None

    def fetch(self, start, end):
        if self.fail:
            raise RuntimeError("api down")
        return self.rows

    def load_bronze(self, path):
        self.loaded = path
        return {"rows_in": len(self.rows), "inserted": len(self.rows), "total": len(self.rows)}


@pytest.fixture
def wm(monkeypatch):
    state = {"wm": None, "set": [], "runs": []}
    monkeypatch.setattr(base_mod, "get_watermark", lambda *_a, **_k: state["wm"])
    monkeypatch.setattr(base_mod, "set_watermark", lambda _s, _src, d, **_k: state["set"].append(d))
    monkeypatch.setattr(base_mod, "write_run", lambda _s, rec, _st=None: state["runs"].append(rec))
    return state


def test_ingestor_run_lands_loads_and_advances_watermark(tmp_path, wm):
    ing = DummyIngestor(FakeSpark(), S, landing_root=str(tmp_path), rows=[{"k": 1}, {"k": 2}])
    res = ing.run("backfill", date(2024, 1, 1), date(2024, 1, 3))
    assert res["inserted"] == 2 and res["window"][0] == "2024-01-01"
    landed = list(tmp_path.rglob("*.jsonl"))
    assert len(landed) == 1 and len(landed[0].read_text().splitlines()) == 2
    assert wm["set"] == [date(2024, 1, 3)] and wm["runs"][0].status == "success" and wm["runs"][0].rows_in == 2


def test_ingestor_watermark_never_regresses(tmp_path, wm):
    wm["wm"] = date(2025, 1, 1)
    DummyIngestor(FakeSpark(), S, landing_root=str(tmp_path), rows=[{"k": 1}]).run("backfill", date(2024, 1, 1), date(2024, 1, 3))
    assert wm["set"] == []  # an old backfill must not pull the watermark back


def test_ingestor_failure_is_logged_and_reraised(tmp_path, wm):
    with pytest.raises(RuntimeError):
        DummyIngestor(FakeSpark(), S, landing_root=str(tmp_path), fail=True).run("backfill", date(2024, 1, 1), date(2024, 1, 2))
    assert wm["runs"][0].status == "failed" and "api down" in wm["runs"][0].error


def test_ingestor_nothing_to_load(tmp_path, wm):
    wm["wm"] = date.today()
    res = DummyIngestor(FakeSpark(), S, landing_root=str(tmp_path)).run("incremental")
    assert res["window"] is None and wm["runs"][0].status == "success"


# ----------------------------------------------------------------------------------------------- source fetch paths (fake HTTP)
class FakeClient:
    def __init__(self, payloads):
        self.payloads, self.calls = list(payloads), []

    def get_json(self, url, params=None):
        self.calls.append((url, params))
        return self.payloads.pop(0) if len(self.payloads) > 1 else self.payloads[0]


def _load(name):
    return json.loads((FX / name).read_text())


def test_openmeteo_fetch_batches_and_chunks():
    dists = [{"district_id": f"D{i}", "lat": 18 + i, "lon": 73 + i} for i in range(2)]
    cli = FakeClient([_load("openmeteo_multi.json")])
    ing = OpenMeteoArchiveIngestor(FakeSpark(), dists, batch_size=2, chunk_days=366, settings=S, client=cli)
    rows = ing.fetch(date(2024, 1, 1), date(2024, 1, 2))
    assert len(rows) == 4 and cli.calls[0][1]["latitude"] == "18,19" and rows[0]["_request_params"] == "2024-01-01..2024-01-02"
    assert OpenMeteoArchiveIngestor.request_weight(10, 366) == pytest.approx(10 * 366 / 14)
    assert OpenMeteoArchiveIngestor.request_weight(5, 7) == 5  # never below 1 per location


def test_openmeteo_forecast_fetch_sets_issue_date():
    dists = [{"district_id": "D0", "lat": 18, "lon": 73}]
    cli = FakeClient([_load("openmeteo_multi.json")[0]])
    rows = OpenMeteoForecastIngestor(FakeSpark(), dists, batch_size=1, settings=S, client=cli).fetch(date.today(), date.today())
    from datetime import datetime, timezone

    assert rows and rows[0]["issued_date"] == datetime.now(timezone.utc).date().isoformat()  # issue date is UTC by design


def test_nasa_fetch_one_request_per_district():
    dists = [{"district_id": "D1", "lat": 18, "lon": 73}, {"district_id": "D2", "lat": 19, "lon": 74}]
    cli = FakeClient([_load("nasa_power.json")])
    rows = NasaPowerIngestor(FakeSpark(), dists, workers=2, settings=S, client=cli).fetch(date(2024, 1, 1), date(2024, 1, 2))
    assert len(cli.calls) == 2 and {r["district_id"] for r in rows} == {"D1", "D2"} and len(rows) == 4


def test_agmarknet_api_paging_and_date_filter():
    page = _load("agmarknet_page.json")
    cli = FakeClient([page, {"records": []}])  # first commodity: one page then empty; later commodities: empty
    s = get_settings(commodities=["Wheat"])
    ing = AgmarknetIngestor(FakeSpark(), s, client=cli, page_size=1000, api_key="k")
    rows = ing.fetch(date(2024, 1, 1), date(2024, 12, 31))
    assert len(rows) == 2 and cli.calls[0][1]["api-key"] == "k" and cli.calls[0][1]["filters[commodity]"] == "Wheat"
    assert ing.fetch(date(2030, 1, 1), date(2030, 1, 2)) == []
