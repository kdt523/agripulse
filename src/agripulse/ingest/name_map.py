"""District-name reconciliation across sources (the #1 real-world issue with Indian datasets).

Mandi sources spell districts differently from the DataMeet Census-2011 names (renames, splits, spelling).
Strategy: manual overrides first -> exact normalised match within state -> fuzzy match within state
(rapidfuzz WRatio >= threshold) -> fuzzy across states (higher threshold) -> unmatched (kept, documented).
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass

STATE_ALIASES = {  # post-2011 / alternate spellings -> DataMeet ST_NM
    "UTTARAKHAND": "UTTARAKHAND", "UTTRAKHAND": "UTTARAKHAND", "UTTARANCHAL": "UTTARAKHAND",
    "ORISSA": "ODISHA", "PONDICHERRY": "PUDUCHERRY", "CHATTISGARH": "CHHATTISGARH",
    "TELANGANA": "ANDHRA PRADESH",  # Census 2011 predates the split
    "NCT OF DELHI": "DELHI", "JAMMU AND KASHMIR": "JAMMU & KASHMIR",
}  # fmt: skip


def norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9 ]+", " ", s or "")).strip().upper()


def norm_state(s: str | None) -> str:
    n = norm(s).replace(" AND ", " & ")
    return STATE_ALIASES.get(n, n)


@dataclass
class Match:
    src_state: str
    src_district: str
    district_id: str | None
    score: float
    method: str


def load_overrides(path: str) -> dict[tuple[str, str], str]:
    """CSV with columns src_state,src_district,district_id -> {(NORM STATE, NORM DISTRICT): district_id}."""
    out: dict[tuple[str, str], str] = {}
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                out[(norm_state(r["src_state"]), norm(r["src_district"]))] = r["district_id"].strip()
    except FileNotFoundError:
        pass
    return out


def match_names(
    sources: list[tuple[str, str]],
    geo: list[dict],
    overrides: dict[tuple[str, str], str] | None = None,
    threshold: float = 88.0,
    cross_state_threshold: float = 95.0,
) -> list[Match]:
    """sources: [(state, district)] from a mandi feed; geo: [{district_id, district_name, state_name}]."""
    from rapidfuzz import fuzz, process

    overrides = overrides or {}
    by_state: dict[str, dict[str, str]] = {}
    all_names: dict[str, str] = {}
    for g in geo:
        st, nm = norm_state(g["state_name"]), norm(g["district_name"])
        by_state.setdefault(st, {})[nm] = g["district_id"]
        all_names[f"{nm}|{st}"] = g["district_id"]

    out: list[Match] = []
    for state, district in sources:
        st, nm = norm_state(state), norm(district)
        if (st, nm) in overrides:
            out.append(Match(state, district, overrides[(st, nm)], 100.0, "override"))
            continue
        cands = by_state.get(st, {})
        if nm in cands:
            out.append(Match(state, district, cands[nm], 100.0, "exact"))
            continue
        if cands:
            hit = process.extractOne(nm, list(cands), scorer=fuzz.WRatio)
            if hit and hit[1] >= threshold:
                out.append(Match(state, district, cands[hit[0]], float(hit[1]), "fuzzy_state"))
                continue
        hit = process.extractOne(nm, list(all_names), scorer=fuzz.WRatio)
        if hit and hit[1] >= cross_state_threshold and hit[0].split("|")[0] == nm:
            out.append(Match(state, district, all_names[hit[0]], float(hit[1]), "fuzzy_any_state"))
        else:
            out.append(Match(state, district, None, float(hit[1]) if hit else 0.0, "unmatched"))
    return out
