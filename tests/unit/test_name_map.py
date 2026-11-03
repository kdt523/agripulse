from agripulse.ingest.name_map import match_names, norm_state

GEO = [
    {"district_id": "D1", "district_name": "Pune", "state_name": "Maharashtra"},
    {"district_id": "D2", "district_name": "Ahmadnagar", "state_name": "Maharashtra"},
    {"district_id": "D3", "district_name": "Rangareddy", "state_name": "Andhra Pradesh"},
    {"district_id": "D4", "district_name": "Dakshin Bastar Dantewada", "state_name": "Chhattisgarh"},
]


def test_exact_fuzzy_unmatched_and_override():
    res = match_names(
        [("Maharashtra", "PUNE"), ("Maharashtra", "Ahmednagar"), ("Telangana", "Ranga Reddy"),
         ("Maharashtra", "Atlantis"), ("Chattisgarh", "Dantewada")],
        GEO,
        overrides={("CHHATTISGARH", "DANTEWADA"): "D4"},
    )  # fmt: skip
    m = {r.src_district: r for r in res}
    assert m["PUNE"].method == "exact" and m["PUNE"].district_id == "D1"
    assert m["Ahmednagar"].district_id == "D2" and m["Ahmednagar"].method == "fuzzy_state"
    assert m["Ranga Reddy"].district_id == "D3"  # Telangana mapped onto Census-2011 Andhra Pradesh
    assert m["Atlantis"].district_id is None and m["Atlantis"].method == "unmatched"
    assert m["Dantewada"].method == "override"


def test_state_aliases():
    assert norm_state("Orissa") == "ODISHA"
    assert norm_state("Jammu and Kashmir") == "JAMMU & KASHMIR"
