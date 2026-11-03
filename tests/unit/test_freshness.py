from datetime import date

from agripulse.quality.freshness import Alert, is_breached, lag_days, post_webhook, slack_payload, volume_anomaly


def test_lag_and_breach():
    assert lag_days(date(2026, 11, 1), date(2026, 11, 4)) == 3
    assert lag_days(None, date(2026, 11, 4)) is None
    assert not is_breached(3, 3)
    assert is_breached(4, 3)
    assert is_breached(0, -1)  # forced test: SLA of -1 always breaches
    assert is_breached(None, 2)  # no data at all
    assert not is_breached(9999, None)  # static archive never breaches


def test_volume_anomaly():
    assert volume_anomaly(10, [100] * 7)
    assert not volume_anomaly(60, [100] * 7)
    assert not volume_anomaly(0, [])  # no history -> no alert


def test_slack_payload_and_post():
    alerts = [Alert("freshness", "weather", "lag 5d exceeds SLA 3d", "https://x/run/1")]
    p = slack_payload(alerts)
    assert "weather" in p["text"] and "https://x/run/1" in p["text"]

    class S:
        def post(self, url, data, headers, timeout):
            self.sent = (url, data)

            class R:
                status_code = 200

            return R()

    s = S()
    assert post_webhook("https://hooks.example/abc", alerts, session=s)
    assert "weather" in s.sent[1]
