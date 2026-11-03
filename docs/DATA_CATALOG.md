# Data catalogue

_Generated from `docs/catalog.yml` + Unity Catalog `information_schema` by `quality/catalog_export.py`. Do not edit by hand._

## Bronze layer

### `agripulse.bronze.geo_districts`

Census-2011 district polygons reduced to centroid, area and simplified WKT. One row per district (641).

- **Source:** https://github.com/datameet/maps (website/docs/data/geojson/dists11.geojson)
- **Licence:** CC BY 4.0 (DataMeet)
- **Refresh:** static  |  **Freshness SLA:** n/a
- **Grain:** district  |  **Keys:** district_id  |  **Owner:** kd  |  PII: false
- **Known issues:** Census-2011 boundaries - post-2011 districts and Telangana are not separate; names differ from modern spellings.
- **Lineage:** DataMeet GeoJSON -> bronze.geo_districts -> silver.districts -> gold.district_week / district_profile

| Column | Type | Description |
|---|---|---|
| `area_km2` | DOUBLE | District area, km2 |
| `census_code` | LONG | Census-2011 district code |
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `district_name` | STRING | District name (Census 2011) |
| `geometry_wkt` | STRING | Simplified district polygon as WKT |
| `lat` | DOUBLE | Latitude of the district centroid |
| `lon` | DOUBLE | Longitude of the district centroid |
| `state_name` | STRING | State name (Census 2011) |
| `_ingested_at` | TIMESTAMP | UTC time the row was loaded into bronze |
| `_source` | STRING | Source system label |

```sql
SELECT * FROM agripulse.bronze.geo_districts LIMIT 10;
```

### `agripulse.bronze.weather_daily_raw`

Open-Meteo archive (ERA5) daily weather per district centroid, as returned by the API.

- **Source:** https://open-meteo.com/en/docs/historical-weather-api
- **Licence:** CC BY 4.0 (Open-Meteo, non-commercial free tier)
- **Refresh:** daily (catch-up job, quota aware)  |  **Freshness SLA:** 3 days
- **Grain:** district x date  |  **Keys:** district_id, date  |  **Owner:** kd  |  PII: false
- **Known issues:** Free-tier quota (~10k weighted calls/day) limits the backfill; history starts 2026-05-01 plus test slices.
- **Lineage:** Open-Meteo archive -> bronze.weather_daily_raw -> silver.weather_daily -> gold.district_week

| Column | Type | Description |
|---|---|---|
| `_request_params` | STRING | Request window or parameters that produced the row |
| `date` | DATE | Observation date |
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `et0_fao_evapotranspiration` | DOUBLE | FAO-56 reference evapotranspiration, mm/day |
| `precipitation_sum` | DOUBLE | Daily precipitation total, mm |
| `rain_sum` | DOUBLE | Daily rain total, mm |
| `relative_humidity_2m_mean` | LONG | Mean relative humidity at 2 m, percent |
| `temperature_2m_max` | DOUBLE | Daily maximum temperature at 2 m, degC |
| `temperature_2m_min` | DOUBLE | Daily minimum temperature at 2 m, degC |
| `wind_speed_10m_max` | DOUBLE | Daily maximum wind speed at 10 m, km/h |
| `_ingested_at` | TIMESTAMP | UTC time the row was loaded into bronze |
| `_source` | STRING | Source system label |
| `_run_id` | STRING | Ingest run id (joins to ops.pipeline_runs) |

```sql
SELECT * FROM agripulse.bronze.weather_daily_raw LIMIT 10;
```

### `agripulse.bronze.weather_forecast_raw`

Open-Meteo 16-day forecast per district, kept per issue date so past forecasts are never overwritten.

- **Source:** https://open-meteo.com/en/docs
- **Licence:** CC BY 4.0 (Open-Meteo)
- **Refresh:** daily  |  **Freshness SLA:** 1 day
- **Grain:** district x forecast date x issued date  |  **Keys:** district_id, date, issued_date  |  **Owner:** kd  |  PII: false
- **Known issues:** none
- **Lineage:** Open-Meteo forecast -> bronze.weather_forecast_raw -> silver.weather_daily (is_forecast) -> gold.district_week

| Column | Type | Description |
|---|---|---|
| `_request_params` | STRING | Request window or parameters that produced the row |
| `date` | DATE | Observation date |
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `et0_fao_evapotranspiration` | DOUBLE | FAO-56 reference evapotranspiration, mm/day |
| `issued_date` | DATE | Date the forecast was issued |
| `precipitation_sum` | DOUBLE | Daily precipitation total, mm |
| `rain_sum` | DOUBLE | Daily rain total, mm |
| `relative_humidity_2m_mean` | LONG | Mean relative humidity at 2 m, percent |
| `temperature_2m_max` | DOUBLE | Daily maximum temperature at 2 m, degC |
| `temperature_2m_min` | DOUBLE | Daily minimum temperature at 2 m, degC |
| `wind_speed_10m_max` | DOUBLE | Daily maximum wind speed at 10 m, km/h |
| `_ingested_at` | TIMESTAMP | UTC time the row was loaded into bronze |
| `_source` | STRING | Source system label |
| `_run_id` | STRING | Ingest run id (joins to ops.pipeline_runs) |

```sql
SELECT * FROM agripulse.bronze.weather_forecast_raw LIMIT 10;
```

### `agripulse.bronze.nasa_power_raw`

NASA POWER (MERRA-2 / CERES) daily agro-climate per district centroid. Fill value -999 is kept raw.

- **Source:** https://power.larc.nasa.gov/docs/services/api/temporal/daily/
- **Licence:** Public domain (NASA)
- **Refresh:** daily  |  **Freshness SLA:** 5 days
- **Grain:** district x date  |  **Keys:** district_id, date  |  **Owner:** kd  |  PII: false
- **Known issues:** Publication lag ~4 days; -999 fill values (quarantined in silver); 0.5 degree grid, so mountain districts are smoothed.
- **Lineage:** NASA POWER -> bronze.nasa_power_raw -> silver.agro_climate_daily + silver.weather_daily (fallback) -> gold.district_week

| Column | Type | Description |
|---|---|---|
| `_request_params` | STRING | Request window or parameters that produced the row |
| `allsky_sfc_sw_dwn` | DOUBLE | NASA all-sky surface shortwave downward irradiance, MJ/m2/day (-999 = missing) |
| `date` | DATE | Observation date |
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `gwetroot` | DOUBLE | NASA root-zone soil wetness, 0-1 |
| `gwettop` | DOUBLE | NASA surface soil wetness, 0-1 |
| `prectotcorr` | DOUBLE | NASA bias-corrected daily precipitation, mm |
| `rh2m` | DOUBLE | NASA relative humidity at 2 m, percent |
| `t2m` | DOUBLE | NASA mean temperature at 2 m, degC |
| `t2m_max` | DOUBLE | NASA daily maximum temperature at 2 m, degC |
| `t2m_min` | DOUBLE | NASA daily minimum temperature at 2 m, degC |
| `ws10m` | DOUBLE | NASA wind speed at 10 m, m/s |
| `_ingested_at` | TIMESTAMP | UTC time the row was loaded into bronze |
| `_source` | STRING | Source system label |
| `_run_id` | STRING | Ingest run id (joins to ops.pipeline_runs) |

```sql
SELECT * FROM agripulse.bronze.nasa_power_raw LIMIT 10;
```

### `agripulse.bronze.mandi_prices_raw`

Agmarknet mandi (APMC) daily prices and arrivals per market, variety and grade, raw spellings kept.

- **Source:** https://agmarknet.gov.in (public archive github.com/iancovert/Agmarknet; live API api.data.gov.in resource 9ef84268-d588-465a-a308-a864a43d0070)
- **Licence:** Government of India open data (NDSAP); archive mirror public on GitHub
- **Refresh:** static archive 2013-2017 loaded; live API via ADF landing  |  **Freshness SLA:** n/a (static archive)
- **Grain:** market x commodity x variety x grade x date  |  **Keys:** state, district, market, commodity, variety, grade, arrival_date  |  **Owner:** kd  |  PII: false
- **Known issues:** Archive ends 2017-06; min/modal/max inconsistent in ~7% of rows; district spellings differ from Census names.
- **Lineage:** Agmarknet (ADF / archive) -> bronze.mandi_prices_raw -> silver.mandi_prices_daily -> gold.district_week

| Column | Type | Description |
|---|---|---|
| `state` | STRING | State name |
| `district` | STRING | District name as reported by the source |
| `market` | STRING | Market (mandi) name as reported |
| `commodity` | STRING | Commodity name as reported |
| `variety` | STRING | Commodity variety as reported |
| `grade` | STRING | Commodity grade as reported |
| `arrival_date` | DATE | Date of the market report |
| `min_price` | DOUBLE | Minimum price, INR/quintal, as reported |
| `max_price` | DOUBLE | Maximum price, INR/quintal, as reported |
| `modal_price` | DOUBLE | Modal price, INR/quintal, as reported |
| `arrivals_tonnes` | DOUBLE | Daily market arrivals as reported, tonnes |
| `_ingested_at` | TIMESTAMP | UTC time the row was loaded into bronze |
| `_source` | STRING | Source system label |
| `_run_id` | STRING | Ingest run id (joins to ops.pipeline_runs) |
| `_request_params` | STRING | Request window or parameters that produced the row |

```sql
SELECT * FROM agripulse.bronze.mandi_prices_raw LIMIT 10;
```

## Silver layer

### `agripulse.silver.districts`

Standardised districts (upper-case names, centroid, area).

- **Source:** datameet
- **Licence:** n/a
- **Refresh:** weekly  |  **Freshness SLA:** n/a
- **Grain:** district  |  **Keys:** district_id  |  **Owner:** kd  |  PII: false

**Quality rules** (violating rows go to the `_quarantine` table):

- `valid_key`: `district_id IS NOT NULL`
- `valid_coords`: `lat BETWEEN 6 AND 38 AND lon BETWEEN 68 AND 98`
- `area_positive`: `area_km2 > 0`

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `district_name_std` | STRING | Upper-case trimmed district name |
| `state_std` | STRING | Upper-case trimmed state name |
| `lat` | DOUBLE | Latitude of the district centroid |
| `lon` | DOUBLE | Longitude of the district centroid |
| `area_km2` | DOUBLE | District area, km2 |

```sql
SELECT * FROM agripulse.silver.districts LIMIT 10;
```

### `agripulse.silver.districts_quarantine`

District rows rejected by quality rules, with the failed rule names.

- **Source:** datameet
- **Licence:** n/a
- **Refresh:** with silver pipeline  |  **Freshness SLA:** n/a
- **Grain:** district  |  **Keys:** district_id  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `district_name_std` | STRING | Upper-case trimmed district name |
| `state_std` | STRING | Upper-case trimmed state name |
| `lat` | DOUBLE | Latitude of the district centroid |
| `lon` | DOUBLE | Longitude of the district centroid |
| `area_km2` | DOUBLE | District area, km2 |
| `failed_rules` | ARRAY | Names of the quality rules the row violated |

```sql
SELECT * FROM agripulse.silver.districts_quarantine LIMIT 10;
```

### `agripulse.silver.weather_daily`

Daily district weather in degC / mm / % / km/h. Open-Meteo preferred, NASA POWER fallback (weather_source). is_forecast marks forecast rows.

- **Source:** open_meteo
- **Licence:** n/a
- **Refresh:** daily  |  **Freshness SLA:** 3 days
- **Grain:** district x date x is_forecast  |  **Keys:** district_id, date  |  **Owner:** kd  |  PII: false
- **Known issues:** et0_mm is NULL for NASA-sourced rows.

**Quality rules** (violating rows go to the `_quarantine` table):

- `valid_key`: `district_id IS NOT NULL AND date IS NOT NULL`
- `temp_present`: `tmax_c IS NOT NULL AND tmin_c IS NOT NULL`
- `tmax_ge_tmin`: `tmax_c >= tmin_c`
- `temp_range`: `tmax_c BETWEEN -50 AND 55 AND tmin_c BETWEEN -60 AND 50`
- `precip_nonneg`: `precip_mm IS NULL OR precip_mm >= 0`
- `date_not_future`: `is_forecast OR date <= current_date()`

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `date` | DATE | Observation date |
| `tmax_c` | DOUBLE | Daily maximum temperature, degC |
| `tmin_c` | DOUBLE | Daily minimum temperature, degC |
| `precip_mm` | DOUBLE | Precipitation, mm |
| `et0_mm` | DOUBLE | Reference evapotranspiration, mm/day |
| `rh_pct` | DOUBLE | Relative humidity, percent |
| `wind_kmh` | DOUBLE | Daily maximum wind speed at 10 m, km/h |
| `is_forecast` | BOOLEAN | True for forecast rows |
| `weather_source` | STRING | Which source supplied the row - open_meteo, nasa_power or open_meteo_forecast |

```sql
SELECT * FROM agripulse.silver.weather_daily LIMIT 10;
```

### `agripulse.silver.weather_daily_quarantine`

Weather rows rejected by quality rules, with the failed rule names.

- **Source:** open_meteo
- **Licence:** n/a
- **Refresh:** with silver pipeline  |  **Freshness SLA:** n/a
- **Grain:** district x date  |  **Keys:** district_id, date  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `date` | DATE | Observation date |
| `tmax_c` | DOUBLE | Daily maximum temperature, degC |
| `tmin_c` | DOUBLE | Daily minimum temperature, degC |
| `precip_mm` | DOUBLE | Precipitation, mm |
| `et0_mm` | DOUBLE | Reference evapotranspiration, mm/day |
| `rh_pct` | DOUBLE | Relative humidity, percent |
| `wind_kmh` | DOUBLE | Daily maximum wind speed at 10 m, km/h |
| `is_forecast` | BOOLEAN | True for forecast rows |
| `weather_source` | STRING | Which source supplied the row - open_meteo, nasa_power or open_meteo_forecast |
| `failed_rules` | ARRAY | Names of the quality rules the row violated |

```sql
SELECT * FROM agripulse.silver.weather_daily_quarantine LIMIT 10;
```

### `agripulse.silver.agro_climate_daily`

NASA POWER daily agro-climate - solar radiation, root/top-zone soil wetness, temperature, precipitation.

- **Source:** nasa_power
- **Licence:** n/a
- **Refresh:** daily  |  **Freshness SLA:** 5 days
- **Grain:** district x date  |  **Keys:** district_id, date  |  **Owner:** kd  |  PII: false

**Quality rules** (violating rows go to the `_quarantine` table):

- `valid_key`: `district_id IS NOT NULL AND date IS NOT NULL`
- `nasa_fill`: `solar_mj_m2 IS NOT NULL AND solar_mj_m2 != -999`
- `soil_wetness_range`: `soil_wetness_root IS NULL OR soil_wetness_root BETWEEN 0 AND 1`
- `precip_nonneg`: `precip_mm IS NULL OR precip_mm >= 0`
- `date_not_future`: `date <= current_date()`

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `date` | DATE | Observation date |
| `solar_mj_m2` | DOUBLE | Solar radiation, MJ/m2/day (-999 = NASA fill value) |
| `soil_wetness_root` | DOUBLE | Root-zone soil wetness, 0-1 |
| `soil_wetness_top` | DOUBLE | Surface soil wetness, 0-1 |
| `t2m_c` | DOUBLE | Mean temperature at 2 m, degC |
| `precip_mm` | DOUBLE | Precipitation, mm |

```sql
SELECT * FROM agripulse.silver.agro_climate_daily LIMIT 10;
```

### `agripulse.silver.agro_climate_daily_quarantine`

Agro-climate rows rejected by quality rules (e.g. NASA -999 fill), with failed rule names.

- **Source:** nasa_power
- **Licence:** n/a
- **Refresh:** with silver pipeline  |  **Freshness SLA:** n/a
- **Grain:** district x date  |  **Keys:** district_id, date  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `date` | DATE | Observation date |
| `solar_mj_m2` | DOUBLE | Solar radiation, MJ/m2/day (-999 = NASA fill value) |
| `soil_wetness_root` | DOUBLE | Root-zone soil wetness, 0-1 |
| `soil_wetness_top` | DOUBLE | Surface soil wetness, 0-1 |
| `t2m_c` | DOUBLE | Mean temperature at 2 m, degC |
| `precip_mm` | DOUBLE | Precipitation, mm |
| `failed_rules` | ARRAY | Names of the quality rules the row violated |

```sql
SELECT * FROM agripulse.silver.agro_climate_daily_quarantine LIMIT 10;
```

### `agripulse.silver.mandi_prices_daily`

Standardised mandi prices (INR/quintal) and arrivals (tonnes), commodity names normalised, district_id via ops.district_name_map.

- **Source:** agmarknet
- **Licence:** n/a
- **Refresh:** when new mandi files land  |  **Freshness SLA:** n/a (static archive)
- **Grain:** market x commodity x variety x date  |  **Keys:** market_id, commodity_std, variety_std, date  |  **Owner:** kd  |  PII: false

**Quality rules** (violating rows go to the `_quarantine` table):

- `valid_key`: `district_id IS NOT NULL AND market_id IS NOT NULL AND commodity_std IS NOT NULL AND date IS NOT NULL`
- `price_positive`: `modal_price_inr_qtl > 0`
- `price_order`: `min_price_inr_qtl <= modal_price_inr_qtl AND modal_price_inr_qtl <= max_price_inr_qtl`
- `date_not_future`: `date <= current_date()`
- `price_spike_extreme`: `rolling_median_price IS NULL OR modal_price_inr_qtl < 50 * rolling_median_price`
- `arrivals_plausible`: `arrivals_t IS NULL OR (arrivals_t >= 0 AND arrivals_t <= 100000)`
- `price_spike (warn)`: `rolling_median_price IS NULL OR modal_price_inr_qtl < 20 * rolling_median_price`

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `market_id` | STRING | Market key - district_id plus normalised market name |
| `commodity_std` | STRING | Standardised commodity name |
| `variety_std` | STRING | Upper-case trimmed variety |
| `date` | DATE | Observation date |
| `min_price_inr_qtl` | DOUBLE | Minimum price, INR per quintal |
| `max_price_inr_qtl` | DOUBLE | Maximum price, INR per quintal |
| `modal_price_inr_qtl` | DOUBLE | Modal price, INR per quintal |
| `arrivals_t` | DOUBLE | Daily market arrivals, tonnes |
| `rolling_median_price` | DOUBLE | Median modal price over the previous 30 observations of the same market, commodity and variety |

```sql
SELECT * FROM agripulse.silver.mandi_prices_daily LIMIT 10;
```

### `agripulse.silver.mandi_prices_daily_quarantine`

Mandi rows rejected by quality rules (price order, non-positive price, unmatched district), with failed rule names.

- **Source:** agmarknet
- **Licence:** n/a
- **Refresh:** with silver pipeline  |  **Freshness SLA:** n/a
- **Grain:** market x commodity x variety x date  |  **Keys:** market_id, commodity_std, variety_std, date  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `market_id` | STRING | Market key - district_id plus normalised market name |
| `commodity_std` | STRING | Standardised commodity name |
| `variety_std` | STRING | Upper-case trimmed variety |
| `date` | DATE | Observation date |
| `min_price_inr_qtl` | DOUBLE | Minimum price, INR per quintal |
| `max_price_inr_qtl` | DOUBLE | Maximum price, INR per quintal |
| `modal_price_inr_qtl` | DOUBLE | Modal price, INR per quintal |
| `arrivals_t` | DOUBLE | Daily market arrivals, tonnes |
| `rolling_median_price` | DOUBLE | Median modal price over the previous 30 observations of the same market, commodity and variety |
| `failed_rules` | ARRAY | Names of the quality rules the row violated |

```sql
SELECT * FROM agripulse.silver.mandi_prices_daily_quarantine LIMIT 10;
```

### `agripulse.silver.dq_duplicate_keys`

Natural keys appearing more than once in bronze mandi data (uniqueness check; expected empty).

- **Source:** agmarknet
- **Licence:** n/a
- **Refresh:** with silver pipeline  |  **Freshness SLA:** n/a
- **Grain:** natural key  |  **Keys:** state, district, market, commodity, variety, grade, arrival_date  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `state` | STRING | State name |
| `district` | STRING | District name as reported by the source |
| `market` | STRING | Market (mandi) name as reported |
| `commodity` | STRING | Commodity name as reported |
| `variety` | STRING | Commodity variety as reported |
| `grade` | STRING | Commodity grade as reported |
| `arrival_date` | DATE | Date of the market report |
| `n` | LONG | Number of times the natural key appears in bronze |

```sql
SELECT * FROM agripulse.silver.dq_duplicate_keys LIMIT 10;
```

## Gold layer

### `agripulse.gold.district_week`

Feature table for demand forecasting - district x ISO week x commodity with mandi demand proxy, weather, agro-climate, forecast and geo features. Leakage-safe.

- **Source:** derived
- **Licence:** n/a
- **Refresh:** daily incremental (weeks since watermark - 8)  |  **Freshness SLA:** 1 day after silver
- **Grain:** district x week x commodity  |  **Keys:** district_id, week_start, commodity_std  |  **Owner:** kd  |  PII: false
- **Known issues:** et0_sum is NULL where only NASA weather exists; precip_forecast_next_7d only for the current week.

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `week_start` | DATE | Monday of the ISO week |
| `commodity_std` | STRING | Standardised commodity name |
| `arrivals_t_sum` | DOUBLE | Weekly mandi arrivals in the district, tonnes (demand proxy) |
| `modal_price_avg` | DOUBLE | Weekly mean modal price, INR per quintal |
| `precip_sum_7d` | DOUBLE | Rainfall in the week, mm |
| `precip_sum_28d` | DOUBLE | Rainfall over this and the previous 3 weeks, mm |
| `precip_anomaly` | DOUBLE | Weekly rainfall minus same-week mean of the previous 5 years, mm |
| `tmax_avg` | DOUBLE | Mean daily maximum temperature in the week, degC |
| `heat_days` | LONG | Days with maximum temperature above 40 degC |
| `dry_spell_len` | LONG | Longest run of consecutive dry days (<1 mm) ending in the week |
| `et0_sum` | DOUBLE | Weekly reference evapotranspiration, mm |
| `gdd` | DOUBLE | Growing degree days, base 10 degC |
| `soil_wetness_root_avg` | DOUBLE | Weekly mean root-zone soil wetness, 0-1 |
| `soil_moisture_anomaly` | DOUBLE | Weekly root-zone wetness minus same-week mean of the previous 5 years |
| `solar_sum` | DOUBLE | Weekly solar radiation, MJ/m2 |
| `precip_forecast_next_7d` | DOUBLE | Forecast rainfall for the 7 days after the build date, mm |
| `arrivals_lag_1w` | DOUBLE | Arrivals 1 week earlier, tonnes |
| `arrivals_lag_2w` | DOUBLE | Arrivals 2 weeks earlier, tonnes |
| `arrivals_lag_4w` | DOUBLE | Arrivals 4 weeks earlier, tonnes |
| `arrivals_lag_52w` | DOUBLE | Arrivals same week last year, tonnes |
| `price_lag_1w` | DOUBLE | Modal price 1 week earlier, INR/quintal |
| `price_lag_2w` | DOUBLE | Modal price 2 weeks earlier, INR/quintal |
| `price_lag_4w` | DOUBLE | Modal price 4 weeks earlier, INR/quintal |
| `price_lag_52w` | DOUBLE | Modal price same week last year, INR/quintal |
| `price_volatility_4w` | DOUBLE | Standard deviation of weekly modal price over 4 weeks, INR/quintal |
| `n_markets` | LONG | Distinct markets reporting in the week |
| `commodity_share_52w` | DOUBLE | Share of this commodity in the district's arrivals over the previous 52 weeks |
| `area_km2` | DOUBLE | District area, km2 |
| `state` | STRING | State name |
| `week_of_year` | INT | ISO week number |
| `year` | INT | Calendar year |
| `season` | STRING | Cropping season - kharif Jun-Oct, rabi Nov-Mar, zaid Apr-May |

```sql
SELECT * FROM agripulse.gold.district_week LIMIT 10;
```

### `agripulse.gold.district_profile`

One row per district - long-run climate, soil wetness, price volatility and mandi crop-mix shares for segmentation.

- **Source:** derived
- **Licence:** n/a
- **Refresh:** daily  |  **Freshness SLA:** 1 day after silver
- **Grain:** district  |  **Keys:** district_id  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `district_name_std` | STRING | Upper-case trimmed district name |
| `state_std` | STRING | Upper-case trimmed state name |
| `lat` | DOUBLE | Latitude of the district centroid |
| `lon` | DOUBLE | Longitude of the district centroid |
| `area_km2` | DOUBLE | District area, km2 |
| `annual_precip_mm` | DOUBLE | Mean annual rainfall over years with >=330 observed days, mm |
| `precip_cv` | DOUBLE | Coefficient of variation of annual rainfall |
| `heat_days_per_year` | DOUBLE | Mean number of days per year with tmax above 40 degC |
| `tmax_mean_c` | DOUBLE | Long-run mean daily maximum temperature, degC |
| `years_used` | LONG | Number of years used in the climatology |
| `soil_wetness_root_mean` | DOUBLE | Long-run mean root-zone soil wetness, 0-1 |
| `price_volatility_cv` | DOUBLE | Mean coefficient of variation of weekly modal price across commodities |
| `share_maize` | DOUBLE | Maize share of the district's mandi arrivals |
| `share_moong` | DOUBLE | Green gram share of the district's mandi arrivals |
| `share_rice` | DOUBLE | Rice share of the district's mandi arrivals |
| `share_soybean` | DOUBLE | Soybean share of the district's mandi arrivals |
| `share_tur` | DOUBLE | Pigeon pea (tur) share of the district's mandi arrivals |
| `share_wheat` | DOUBLE | Wheat share of the district's mandi arrivals |

```sql
SELECT * FROM agripulse.gold.district_profile LIMIT 10;
```

### `agripulse.gold.v_ds_training`

Training view - district_week features at week t plus next-week arrivals and price as targets.

- **Source:** derived
- **Licence:** n/a
- **Refresh:** view  |  **Freshness SLA:** n/a
- **Grain:** district x week x commodity  |  **Keys:** district_id, week_start, commodity_std  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `week_start` | DATE | Monday of the ISO week |
| `commodity_std` | STRING | Standardised commodity name |
| `arrivals_t_sum` | DOUBLE | Weekly mandi arrivals in the district, tonnes (demand proxy) |
| `modal_price_avg` | DOUBLE | Weekly mean modal price, INR per quintal |
| `precip_sum_7d` | DOUBLE | Rainfall in the week, mm |
| `precip_sum_28d` | DOUBLE | Rainfall over this and the previous 3 weeks, mm |
| `precip_anomaly` | DOUBLE | Weekly rainfall minus same-week mean of the previous 5 years, mm |
| `tmax_avg` | DOUBLE | Mean daily maximum temperature in the week, degC |
| `heat_days` | LONG | Days with maximum temperature above 40 degC |
| `dry_spell_len` | LONG | Longest run of consecutive dry days (<1 mm) ending in the week |
| `et0_sum` | DOUBLE | Weekly reference evapotranspiration, mm |
| `gdd` | DOUBLE | Growing degree days, base 10 degC |
| `soil_wetness_root_avg` | DOUBLE | Weekly mean root-zone soil wetness, 0-1 |
| `soil_moisture_anomaly` | DOUBLE | Weekly root-zone wetness minus same-week mean of the previous 5 years |
| `solar_sum` | DOUBLE | Weekly solar radiation, MJ/m2 |
| `precip_forecast_next_7d` | DOUBLE | Forecast rainfall for the 7 days after the build date, mm |
| `arrivals_lag_1w` | DOUBLE | Arrivals 1 week earlier, tonnes |
| `arrivals_lag_2w` | DOUBLE | Arrivals 2 weeks earlier, tonnes |
| `arrivals_lag_4w` | DOUBLE | Arrivals 4 weeks earlier, tonnes |
| `arrivals_lag_52w` | DOUBLE | Arrivals same week last year, tonnes |
| `price_lag_1w` | DOUBLE | Modal price 1 week earlier, INR/quintal |
| `price_lag_2w` | DOUBLE | Modal price 2 weeks earlier, INR/quintal |
| `price_lag_4w` | DOUBLE | Modal price 4 weeks earlier, INR/quintal |
| `price_lag_52w` | DOUBLE | Modal price same week last year, INR/quintal |
| `price_volatility_4w` | DOUBLE | Standard deviation of weekly modal price over 4 weeks, INR/quintal |
| `n_markets` | LONG | Distinct markets reporting in the week |
| `commodity_share_52w` | DOUBLE | Share of this commodity in the district's arrivals over the previous 52 weeks |
| `area_km2` | DOUBLE | District area, km2 |
| `state` | STRING | State name |
| `week_of_year` | INT | ISO week number |
| `year` | INT | Calendar year |
| `season` | STRING | Cropping season - kharif Jun-Oct, rabi Nov-Mar, zaid Apr-May |
| `target_arrivals_next_week` | DOUBLE | Next week's arrivals, tonnes (training target) |
| `target_price_next_week` | DOUBLE | Next week's modal price, INR/quintal (training target) |
| `next_week_arrivals_lag_52w` | DOUBLE | Arrivals one year before the target week (known at prediction time; the seasonal-naive forecast) |

```sql
SELECT * FROM agripulse.gold.v_ds_training LIMIT 10;
```

## Ops layer

### `agripulse.ops.pipeline_runs`

One row per pipeline run (rows in/out/quarantined

- **Source:** internal
- **Licence:** n/a
- **Refresh:** every run  |  **Freshness SLA:** n/a
- **Grain:** run  |  **Keys:** run_id  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | Unique run id |
| `source` | STRING | Data source or pipeline name |
| `layer` | STRING | Medallion layer of the run |
| `started_at` | TIMESTAMP | Run start time (UTC) |
| `duration_s` | DOUBLE | Wall-clock seconds |
| `rows_in` | LONG | Rows read or fetched |
| `rows_out` | LONG | Rows written |
| `rows_quarantined` | LONG | Rows routed to quarantine |
| `status` | STRING | Run status - success or failed |
| `error` | STRING | Error message when the run failed |

```sql
SELECT * FROM agripulse.ops.pipeline_runs LIMIT 10;
```

### `agripulse.ops.ingest_watermarks`

Incremental-load watermarks per source and key (global or per district).

- **Source:** internal
- **Licence:** n/a
- **Refresh:** every ingest  |  **Freshness SLA:** n/a
- **Grain:** source x key  |  **Keys:** source, key  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `source` | STRING | Data source or pipeline name |
| `key` | STRING | Watermark sub-key (global or district_id) |
| `last_date` | DATE | Last date successfully ingested |
| `updated_at` | TIMESTAMP | When the row was last updated (UTC) |

```sql
SELECT * FROM agripulse.ops.ingest_watermarks LIMIT 10;
```

### `agripulse.ops.freshness_sla`

Allowed lag in days per source (NULL = static archive).

- **Source:** internal
- **Licence:** n/a
- **Refresh:** manual  |  **Freshness SLA:** n/a
- **Grain:** source  |  **Keys:** source  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `source` | STRING | Data source or pipeline name |
| `expected_lag_days` | INT | Maximum allowed lag in days (NULL = static archive) |
| `owner` | STRING | Feed owner |

```sql
SELECT * FROM agripulse.ops.freshness_sla LIMIT 10;
```

### `agripulse.ops.freshness_status`

History of freshness checks per source.

- **Source:** internal
- **Licence:** n/a
- **Refresh:** daily  |  **Freshness SLA:** n/a
- **Grain:** source x check time  |  **Keys:** source, checked_at  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `source` | STRING | Data source or pipeline name |
| `checked_at` | TIMESTAMP | When the freshness check ran (UTC) |
| `max_date` | DATE | Latest data date found |
| `lag_days` | INT | Days between the check date and the latest data date |
| `sla_days` | INT | Allowed lag in days at check time |
| `breached` | BOOLEAN | True when lag_days exceeds sla_days |

```sql
SELECT * FROM agripulse.ops.freshness_status LIMIT 10;
```

### `agripulse.ops.alert_log`

Every alert raised (freshness / dq / volume) and whether Slack delivery worked.

- **Source:** internal
- **Licence:** n/a
- **Refresh:** on alert  |  **Freshness SLA:** n/a
- **Grain:** alert  |  **Keys:** alert_ts, source  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `alert_ts` | TIMESTAMP | When the alert was raised (UTC) |
| `kind` | STRING | Alert type - freshness, dq or volume |
| `source` | STRING | Data source or pipeline name |
| `message` | STRING | Human-readable alert detail |
| `delivered` | BOOLEAN | True when the Slack webhook accepted the alert |

```sql
SELECT * FROM agripulse.ops.alert_log LIMIT 10;
```

### `agripulse.ops.dq_scorecard`

Daily data-quality scorecard per silver table.

- **Source:** internal
- **Licence:** n/a
- **Refresh:** daily  |  **Freshness SLA:** n/a
- **Grain:** table x date  |  **Keys:** table, date  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `table` | STRING | Silver table name |
| `date` | DATE | Observation date |
| `rows` | LONG | Total rows before filtering |
| `pass_rows` | LONG | Rows that passed all hard rules |
| `fail_rows` | LONG | Rows quarantined |
| `pass_rate` | DOUBLE | Share of rows passing all hard rules |
| `rules_failed` | MAP | Quarantined rows per failed rule |
| `completeness_pct` | DOUBLE | Mean non-null percentage over the key columns of passing rows |

```sql
SELECT * FROM agripulse.ops.dq_scorecard LIMIT 10;
```

### `agripulse.ops.district_name_map`

Mandi (state

- **Source:** internal
- **Licence:** n/a
- **Refresh:** when mandi data changes  |  **Freshness SLA:** n/a
- **Grain:** source spelling  |  **Keys:** src_state, src_district  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `src_state` | STRING | State spelling in the mandi source |
| `src_district` | STRING | District spelling in the mandi source |
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `score` | DOUBLE | Fuzzy-match score 0-100 |
| `method` | STRING | How the name was matched - override, exact, fuzzy_state, fuzzy_any_state or unmatched |

```sql
SELECT * FROM agripulse.ops.district_name_map LIMIT 10;
```

## Ml layer

### `agripulse.ml.forecast_lift_results`

Forecast-lift experiment metrics per model

- **Source:** derived
- **Licence:** n/a
- **Refresh:** per experiment  |  **Freshness SLA:** n/a
- **Grain:** model x fold x slice  |  **Keys:** model, fold, slice  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `model` | STRING | Model name (seasonal_naive, lgbm_market, lgbm_external) |
| `fold` | STRING | Backtest fold id |
| `slice` | STRING | Metric slice (overall, commodity, state) |
| `wape` | DOUBLE | Weighted absolute percentage error |
| `mape` | DOUBLE | Mean absolute percentage error |
| `rmse` | DOUBLE | Root mean squared error |
| `n_rows` | LONG | Rows evaluated |

```sql
SELECT * FROM agripulse.ml.forecast_lift_results LIMIT 10;
```

### `agripulse.ml.district_segments`

KMeans agro-climatic segment per district with profile.

- **Source:** derived
- **Licence:** n/a
- **Refresh:** per experiment  |  **Freshness SLA:** n/a
- **Grain:** district  |  **Keys:** district_id  |  **Owner:** kd  |  PII: false

| Column | Type | Description |
|---|---|---|
| `district_id` | STRING | Census-2011 district key (D + 4-digit code) |
| `district_name_std` | STRING | Upper-case trimmed district name |
| `state_std` | STRING | Upper-case trimmed state name |
| `segment` | INT | KMeans segment id |
| `segment_name` | STRING | Human-readable segment name |
| `annual_precip_mm` | DOUBLE | Mean annual rainfall over years with >=330 observed days, mm |
| `precip_cv` | DOUBLE | Coefficient of variation of annual rainfall |
| `heat_days_per_year` | DOUBLE | Mean number of days per year with tmax above 40 degC |
| `tmax_mean_c` | DOUBLE | Long-run mean daily maximum temperature, degC |
| `soil_wetness_root_mean` | DOUBLE | Long-run mean root-zone soil wetness, 0-1 |
| `price_volatility_cv` | DOUBLE | Mean coefficient of variation of weekly modal price across commodities |
| `share_maize` | DOUBLE | Maize share of the district's mandi arrivals |
| `share_moong` | DOUBLE | Green gram share of the district's mandi arrivals |
| `share_rice` | DOUBLE | Rice share of the district's mandi arrivals |
| `share_soybean` | DOUBLE | Soybean share of the district's mandi arrivals |
| `share_tur` | DOUBLE | Pigeon pea (tur) share of the district's mandi arrivals |
| `share_wheat` | DOUBLE | Wheat share of the district's mandi arrivals |

```sql
SELECT * FROM agripulse.ml.district_segments LIMIT 10;
```

