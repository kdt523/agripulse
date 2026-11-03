from agripulse.transform import standardise as st


def test_commodity_map_and_names(spark):
    df = spark.createDataFrame(
        [("Paddy(Dhan)(Common)",), ("  wheat ",), ("Soyabean",), ("Dragon Fruit",), ("Arhar (Tur-Red Gram)",)], "commodity string"
    )
    out = [r[0] for r in df.select(st.commodity_map_expr("commodity")).collect()]
    assert out == ["PADDY", "WHEAT", "SOYBEAN", "DRAGON FRUIT", "TUR"]  # unknown names fall back to the upper-cased original


def test_nasa_fill_to_null_and_norm_name(spark):
    df = spark.createDataFrame([(-999.0, "  New   Delhi "), (4.5, "pune")], "v double, n string")
    r = df.select(st.nasa_null_if_fill("v").alias("v"), st.norm_name("n").alias("n")).collect()
    assert r[0]["v"] is None and r[1]["v"] == 4.5
    assert [x["n"] for x in r] == ["NEW DELHI", "PUNE"]
    assert st.KMH_PER_MS == 3.6


def test_schemas_module_exports_bronze_meta():
    from agripulse.common import schemas

    assert "_ingested_at" in schemas.BRONZE_META_COLUMNS
