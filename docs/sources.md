# External data sources: evaluation

Each source was judged on coverage, latency, limits, licence and what was actually observed while building. Alternatives considered are listed so the choice is explicit.

| Source | Why chosen | Coverage | Latency | Limits / observed behaviour | Licence | Alternatives considered |
|---|---|---|---|---|---|---|
| **Open-Meteo** archive + forecast | No API key, multi-location requests, daily variables incl. ET0, 16-day forecast | Global; ERA5 from 1940 | Archive ~2 days; forecast hourly refresh | Free tier ~10k weighted calls/day, weight = locations x days/14. A 25-location x 1-year request timed out; 10 x 1-year succeeded (~60 s). 5-year x 641 districts needs ~9 days of quota, so the backfill is quota-aware and resumable. | CC BY 4.0, non-commercial free tier | IMD gridded data (needs manual request), NASA POWER (used as the long-history fallback), Visual Crossing (key + lower free quota) |
| **NASA POWER** (community AG) | Agronomic / remote-sensing derived indicators (solar radiation, root-zone soil wetness) with no practical quota | Global 0.5 degree, 1981 onward | ~4 days | One request per point (641 requests ran in ~4 minutes for 2021+). -999 fill values. Coarse grid smooths mountain districts. | Public domain | Sentinel-2 NDVI via Google Earth Engine (needs account + heavier ops), MODIS (HDF handling) |
| **Agmarknet** mandi prices | The real market signal for agri-input demand: prices and arrivals per market | 3,000+ APMC markets, 300+ commodities | Daily | `api.data.gov.in` is **not reachable from Databricks Free Edition** (egress probe), and needs a key. History loaded from the public archive (iancovert/Agmarknet, 2008-Jun 2017, 12 crops). ~7% of rows have inconsistent min/modal/max. District spellings differ from Census names. | Government of India open data (NDSAP) | Kaggle Agmarknet dumps (login), India Data Portal CKAN (403 to automated clients), eNAM (different schema) |
| **DataMeet districts** | Open, stable Census-2011 polygons with census codes | 641 districts | static | 2011 boundaries: newer districts are missing and Telangana is part of Andhra Pradesh. | CC BY 4.0 | GADM (heavier, licence restrictions), Survey of India (not open) |
| **ICRISAT District Level Database** | Official crop area / production by district | 311 districts, 1966-2017 | yearly | Requires a manual form export, so it is **not loaded**. Crop mix is derived from mandi arrivals shares instead; `load_crop_area_csv` is ready for the export. | Free for research use | Crop Wise APY (data.desagri.gov.in, needs export) |

## Decisions that follow from the evaluation
- Free Edition cannot call `api.data.gov.in`, so the live Agmarknet path is **Azure Data Factory -> landing volume -> `mandi_landing` ingest**.
- Open-Meteo covers the recent window and the forecast. NASA POWER carries the multi-year history.
- The forecast-lift experiment uses 2013-2017, the window where mandi, NASA and geo data all overlap.
