"""Schema contracts for bronze tables (filled in per source in P1)."""

from __future__ import annotations

# Metadata columns every bronze table carries (design rule, P1)
BRONZE_META_COLUMNS = ["_ingested_at", "_source", "_run_id", "_request_params"]
