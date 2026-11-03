"""Pure-Spark standardisation helpers (units, names) used by the silver DLT pipeline and unit tests."""

from __future__ import annotations

# Commodity name normalisation: source spelling (upper-case) -> standard name.
COMMODITY_MAP = {
    "RICE": "RICE", "PADDY(DHAN)(COMMON)": "PADDY", "PADDY": "PADDY",
    "WHEAT": "WHEAT", "MAIZE": "MAIZE",
    "SOYABEAN": "SOYBEAN", "SOYBEAN": "SOYBEAN",
    "COTTON": "COTTON", "TOMATO": "TOMATO", "ONION": "ONION", "POTATO": "POTATO",
    "ARHAR (TUR-RED GRAM)": "TUR", "ARHAR(TUR/RED GRAM)(WHOLE)": "TUR",
    "GREEN GRAM (MOONG)": "MOONG", "GREEN GRAM (MOONG)(WHOLE)": "MOONG",
}  # fmt: skip

KMH_PER_MS = 3.6


def commodity_map_expr(col: str = "commodity"):
    """Spark column mapping raw commodity -> standard name (unknown -> upper-cased original)."""
    from itertools import chain

    from pyspark.sql import functions as F

    m = F.create_map(*[F.lit(x) for x in chain.from_iterable(COMMODITY_MAP.items())])
    up = F.upper(F.trim(F.col(col)))
    return F.coalesce(m[up], up)


def nasa_null_if_fill(col: str):
    """NASA POWER encodes missing as -999; return NULL for those in derived columns."""
    from pyspark.sql import functions as F

    return F.when(F.col(col) == -999, None).otherwise(F.col(col))


def norm_name(col: str):
    """Trim, collapse whitespace, upper-case: the join form of a district/market name."""
    from pyspark.sql import functions as F

    return F.upper(F.regexp_replace(F.trim(F.col(col)), r"\s+", " "))
