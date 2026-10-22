import math

FIRMS_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{source}/world/1/{date}"

SATELLITES = {
    "N20": {"satellite_name": "NOAA-20", "source": "VIIRS_NOAA20_NRT", "instrument": "VIIRS"},
}

RAW_COLUMNS = [
    "latitude", "longitude", "bright_ti4", "scan", "track", "acq_date",
    "acq_time", "satellite", "instrument", "confidence", "version",
    "bright_ti5", "frp", "daynight",
]

RAW_KEY = "raw/date={date}/satellite_{code}.csv"
TRANSFORMED_PREFIX = "transformed/date={date}/"
TABLES_PREFIX = "tables/date={date}/"
COUNTRIES_KEY = "reference/countries.csv"
DIM_COUNTRY_KEY = "reference/dim_country.csv"
DIM_SATELLITE_KEY = "reference/dim_satellite.csv"

MISSING_VALUES = {"", "nan", "null", "none", "-"}

NUMERIC_RANGES = {
    "latitude": (-90, 90),
    "longitude": (-180, 180),
    "bright_ti4": (150, 400),
    "bright_ti5": (150, 450),
    "scan": (0.1, 1.5),
    "track": (0.1, 1.5),
    "frp": (0, math.inf),
}

VALID_CONFIDENCE = {"l", "n", "h"}
VALID_DAYNIGHT = {"D", "N"}

VERSION_PATTERN = r"^(\d+\.\d+)([A-Za-z]+)$"
KEEP_LATENCY_CLASS = "NRT"

CONFIDENCE_LEVELS = {"l": "low", "n": "nominal", "h": "high"}
DAYNIGHT_IS_DAYTIME = {"D": True, "N": False}

UNKNOWN_COUNTRY = {"country_iso3": "UNK", "country_name": "Unknown/Ocean", "continent": "Unknown"}

FACT_COLUMNS = [
    "detection_id", "detected_at_utc", "country_iso3", "satellite", "instrument",
    "latitude", "longitude", "brightness_ti4_k", "brightness_ti5_k", "frp_mw",
    "scan_km", "track_km", "pixel_area_km2", "confidence_level", "is_daytime",
    "product_version", "batch_id",
]
REJECTED_COLUMNS = ["batch_id"] + RAW_COLUMNS + ["reject_reason"]
RUN_SUMMARY_COLUMNS = [
    "batch_id", "processed_at", "rows_in", "rows_rejected", "rows_clean",
    "rows_filtered", "rows_duplicate",
]
DIM_COUNTRY_COLUMNS = ["country_iso3", "country_name", "continent"]
DIM_SATELLITE_COLUMNS = ["satellite", "satellite_name", "instrument", "source"]