"""Freshness SLA, DQ-drop and volume-anomaly checks -> ops.freshness_status, ops.alert_log, Slack webhook.

Pure functions (lag, breach, volume anomaly, payload) are unit-tested; `run_checks` wires them to Spark.
A source with no SLA (expected_lag_days IS NULL) is a static archive and is reported as 'static', never breached.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

from agripulse.common.config import Settings, get_secret, get_settings
from agripulse.common.logging import get_logger

log = get_logger("agripulse.freshness")

DQ_PASS_RATE_MIN = 0.95
VOLUME_DROP_THRESHOLD = 0.5  # alert if today's rows < 50% of the trailing 7-run average


def lag_days(max_date: date | None, today: date) -> int | None:
    return None if max_date is None else (today - max_date).days


def is_breached(lag: int | None, sla_days: int | None) -> bool:
    if sla_days is None:  # static archive
        return False
    return lag is None or lag > sla_days


def volume_anomaly(today_rows: int, previous_rows: list[int]) -> bool:
    """True if today's volume fell by more than 50% against the average of up to the last 7 runs."""
    prev = [r for r in previous_rows[-7:] if r is not None]
    if not prev:
        return False
    avg = sum(prev) / len(prev)
    return avg > 0 and today_rows < (1 - VOLUME_DROP_THRESHOLD) * avg


@dataclass
class Alert:
    kind: str  # freshness | dq | volume
    source: str
    message: str
    run_url: str | None = None


def slack_payload(alerts: list[Alert]) -> dict:
    lines = [f":rotating_light: *AgriPulse* - {len(alerts)} alert(s)"]
    for a in alerts:
        lines.append(f"- *{a.kind}* `{a.source}`: {a.message}")
    if alerts and alerts[0].run_url:
        lines.append(f"<{alerts[0].run_url}|Open run>")
    return {"text": "\n".join(lines)}


def post_webhook(url: str, alerts: list[Alert], session=None) -> bool:
    import requests

    resp = (session or requests).post(url, data=json.dumps(slack_payload(alerts)), headers={"Content-Type": "application/json"}, timeout=15)
    return resp.status_code == 200


# source -> (SQL for max observed date)
def _source_queries(c: str) -> dict[str, str]:
    return {
        "weather": f"SELECT max(date) FROM {c}.silver.weather_daily WHERE NOT is_forecast AND weather_source = 'open_meteo'",
        "nasa_power": f"SELECT max(date) FROM {c}.silver.agro_climate_daily",
        "mandi": f"SELECT max(date) FROM {c}.silver.mandi_prices_daily",
        "forecast": f"SELECT max(issued_date) FROM {c}.bronze.weather_forecast_raw",
    }


def run_checks(spark, settings: Settings | None = None, today: date | None = None, sla_override: dict | None = None,
               run_url: str | None = None, send: bool = True) -> dict:  # fmt: skip
    s = settings or get_settings()
    today = today or date.today()
    c = s.catalog
    sla = {r["source"]: r["expected_lag_days"] for r in spark.table(s.table("ops", "freshness_sla")).collect()}
    sla.update(sla_override or {})
    alerts: list[Alert] = []
    rows = []
    for source, q in _source_queries(c).items():
        max_d = spark.sql(q).collect()[0][0]
        lag = lag_days(max_d, today)
        sla_d = sla.get(source)
        breached = is_breached(lag, sla_d)
        rows.append((source, datetime.now(timezone.utc), max_d, lag, sla_d, breached))
        if breached:
            alerts.append(Alert("freshness", source, f"lag {lag}d exceeds SLA {sla_d}d (max date {max_d})", run_url))

    # DQ pass-rate gate (latest scorecard row per table)
    sc = spark.sql(
        f"SELECT `table`, pass_rate FROM {s.table('ops', 'dq_scorecard')} "
        f"QUALIFY row_number() OVER (PARTITION BY `table` ORDER BY date DESC) = 1"
    ).collect()
    for r in sc:
        if r["pass_rate"] < DQ_PASS_RATE_MIN:
            alerts.append(Alert("dq", r["table"], f"pass rate {r['pass_rate']:.1%} below {DQ_PASS_RATE_MIN:.0%}", run_url))

    # volume anomaly per ingest source from ops.pipeline_runs
    for source in ("weather", "nasa_power"):
        hist = spark.sql(
            f"SELECT rows_in FROM {s.table('ops', 'pipeline_runs')} WHERE source = '{source}' AND layer = 'bronze' "
            f"AND status = 'success' AND rows_in > 0 ORDER BY started_at DESC LIMIT 8"
        ).collect()
        vals = [h["rows_in"] for h in hist]
        if len(vals) >= 2 and volume_anomaly(vals[0], list(reversed(vals[1:]))):
            alerts.append(Alert("volume", source, f"{vals[0]} rows vs trailing avg of {sum(vals[1:]) / len(vals[1:]):.0f}", run_url))

    schema = "source string, checked_at timestamp, max_date date, lag_days int, sla_days int, breached boolean"
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").saveAsTable(s.table("ops", "freshness_status"))
    if alerts:
        spark.createDataFrame(
            [(datetime.now(timezone.utc), a.kind, a.source, a.message, False) for a in alerts],
            "alert_ts timestamp, kind string, source string, message string, delivered boolean",
        ).write.format("delta").mode("append").saveAsTable(s.table("ops", "alert_log"))
        if send:
            try:
                delivered = post_webhook(get_secret(s.slack_secret_key, s.secret_scope), alerts)
            except Exception as exc:  # no webhook configured / network
                log.warning(f"alert not delivered: {exc}")
                delivered = False
            if delivered:
                spark.sql(f"UPDATE {s.table('ops', 'alert_log')} SET delivered = true WHERE delivered = false")
    return {"checked": len(rows), "alerts": [(a.kind, a.source, a.message) for a in alerts]}
