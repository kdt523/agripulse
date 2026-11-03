"""Central configuration. Values come from env vars (AGRIPULSE_*) or notebook/job widgets.

Secrets are never stored here: they are read from the Databricks secret scope at runtime.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_COMMODITIES = [
    "Paddy(Dhan)(Common)", "Wheat", "Maize", "Soyabean", "Cotton", "Tomato", "Onion", "Potato",
]  # fmt: skip


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGRIPULSE_", extra="ignore")

    catalog: str = "agripulse"
    secret_scope: str = "agripulse"
    datagov_secret_key: str = "datagov_api_key"
    slack_secret_key: str = "slack_webhook"

    commodities: list[str] = Field(default_factory=lambda: list(DEFAULT_COMMODITIES))

    # API endpoints (keep in sync with docs/sources.md)
    open_meteo_archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    open_meteo_forecast_url: str = "https://api.open-meteo.com/v1/forecast"
    nasa_power_url: str = "https://power.larc.nasa.gov/api/temporal/daily/point"
    agmarknet_url: str = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
    districts_geojson_url: str = (
        "https://raw.githubusercontent.com/datameet/maps/master/Districts/Census_2011/2011_Dist.json"
    )

    # HTTP behaviour
    http_timeout_s: float = 30.0
    http_max_retries: int = 5
    http_min_interval_s: float = 0.2  # simple client-side rate limit

    def table(self, schema: str, name: str) -> str:
        """Fully qualified table name."""
        return f"{self.catalog}.{schema}.{name}"

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/bronze/landing"


def get_settings(**overrides) -> Settings:
    return Settings(**overrides)


def get_secret(key: str, scope: str | None = None) -> str:
    """Read a secret from Databricks (dbutils) or fall back to env var AGRIPULSE_SECRET_<KEY> locally."""
    import os

    scope = scope or get_settings().secret_scope
    try:  # running on Databricks
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate()).secrets.get(scope, key)
    except Exception:
        val = os.environ.get(f"AGRIPULSE_SECRET_{key.upper()}")
        if not val:
            env_name = f"AGRIPULSE_SECRET_{key.upper()}"
            raise RuntimeError(f"Secret {scope}/{key} not available (set {env_name} locally)") from None
        return val
