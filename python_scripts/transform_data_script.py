import hashlib
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd

import config
import utils


def read_raw(code, processing_date, raw_dir):
    key = config.RAW_KEY.format(date=processing_date.isoformat(), code=code.lower())
    data = (raw_dir / Path(key).name).read_bytes() if raw_dir else utils.s3_get(key)
    fires = utils.read_csv_as_text(data)
    fires.columns = fires.columns.str.strip().str.lower()
    return fires[config.RAW_COLUMNS]


def check(fires):
    """Return the fires with typed columns, and the first problem found for each fire ('' if none)."""
    typed = fires.copy()
    reason = pd.Series("", index=fires.index)

    def flag(is_invalid, message):
        rows_to_flag = is_invalid & (reason == "")
        reason.loc[rows_to_flag] = message

    for column in fires.columns:
        flag(fires[column].str.lower().isin(config.MISSING_VALUES), f"{column}_missing")

    for column, (low, high) in config.NUMERIC_RANGES.items():
        typed[column] = pd.to_numeric(fires[column], errors="coerce")
        flag(typed[column].isna(), f"{column}_not_a_number")
        flag((typed[column] < low) | (typed[column] > high), f"{column}_out_of_range")
    flag(typed["frp"] <= 0, "frp_not_positive")

    flag(~fires["confidence"].isin(config.VALID_CONFIDENCE), "confidence_not_allowed")
    flag(~fires["daynight"].isin(config.VALID_DAYNIGHT), "daynight_not_allowed")
    flag(~fires["satellite"].isin(config.SATELLITES), "satellite_not_allowed")
    flag(fires["instrument"] != "VIIRS", "instrument_not_allowed")

    day = pd.to_datetime(fires["acq_date"], format="%Y-%m-%d", errors="coerce")
    clock = pd.to_datetime(fires["acq_time"].str.zfill(4), format="%H%M", errors="coerce")
    flag(day.isna(), "acq_date_invalid")
    flag(clock.isna(), "acq_time_invalid")
    minutes_into_day = clock.dt.hour * 60 + clock.dt.minute
    typed["detected_at"] = day + pd.to_timedelta(minutes_into_day, unit="m")

    typed[["product_version", "latency_class"]] = fires["version"].str.extract(config.VERSION_PATTERN)
    flag(typed["product_version"].isna(), "version_bad_format")

    return typed, reason


def detection_id(fires):
    timestamp = fires["detected_at"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    key = timestamp + "|" + fires["latitude"].astype(str) + "|" + fires["longitude"].astype(str) + "|" + fires["satellite"]
    return key.map(lambda text: hashlib.md5(text.encode()).hexdigest())


def country_of(fires):
    if fires.empty:
        return pd.Series(dtype=str, index=fires.index)
    countries = utils.read_csv_as_text(utils.s3_get(config.COUNTRIES_KEY))
    polygons = gpd.GeoDataFrame(
        countries[["country_iso3"]],
        geometry=gpd.GeoSeries.from_wkt(countries["geometry_wkt"], crs="EPSG:4326"),
    )
    points = gpd.GeoDataFrame(
        index=fires.index,
        geometry=gpd.points_from_xy(fires["longitude"], fires["latitude"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(points, polygons, how="left", predicate="within")
    joined = joined[~joined.index.duplicated(keep="first")]
    return joined["country_iso3"].fillna(config.UNKNOWN_COUNTRY["country_iso3"])


def standardise(fires, batch_id):
    clean = pd.DataFrame({
        "detection_id": fires["detection_id"],
        "detected_at_utc": fires["detected_at"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "country_iso3": fires["country_iso3"],
        "satellite": fires["satellite"],
        "instrument": fires["instrument"],
        "latitude": fires["latitude"],
        "longitude": fires["longitude"],
        "brightness_ti4_k": fires["bright_ti4"],
        "brightness_ti5_k": fires["bright_ti5"],
        "frp_mw": fires["frp"],
        "scan_km": fires["scan"],
        "track_km": fires["track"],
        "pixel_area_km2": fires["scan"] * fires["track"],
        "confidence_level": fires["confidence"].map(config.CONFIDENCE_LEVELS),
        "is_daytime": fires["daynight"].map(config.DAYNIGHT_IS_DAYTIME),
        "product_version": fires["product_version"],
        "batch_id": batch_id,
    })
    return clean[config.FACT_COLUMNS]


def write_outputs(batch_id, clean, rejected, summary):
    prefix = config.TRANSFORMED_PREFIX.format(date=batch_id)
    utils.s3_put_csv(prefix + "clean.csv", clean)
    utils.s3_put_csv(prefix + "rejected.csv", rejected)
    utils.s3_put(prefix + "run_summary.json", json.dumps(summary, indent=2).encode("utf-8"))


def run(processing_date, raw_dir=None):
    batch_id = processing_date.isoformat()
    processed_at = pd.Timestamp.now("UTC").isoformat()

    fires = pd.concat([read_raw(code, processing_date, raw_dir) for code in config.SATELLITES], ignore_index=True)
    print(f"Read {len(fires)} raw fires")

    if fires.empty:
        summary = {
            "batch_id": batch_id, "processed_at": processed_at, "rows_in": 0,
            "rows_rejected": 0, "rows_clean": 0, "rows_filtered": 0, "rows_duplicate": 0,
        }
        write_outputs(batch_id, pd.DataFrame(columns=config.FACT_COLUMNS),
                      pd.DataFrame(columns=config.REJECTED_COLUMNS), summary)
        print("No fires today; wrote empty files.")
        return summary

    typed, reason = check(fires)
    is_rejected = reason != ""
    rejected = fires[is_rejected].assign(batch_id=batch_id, reject_reason=reason[is_rejected])
    rejected = rejected[config.REJECTED_COLUMNS]

    valid = typed[~is_rejected]
    nrt = valid[valid["latency_class"] == config.KEEP_LATENCY_CLASS].copy()
    rows_filtered = len(valid) - len(nrt)

    nrt["detection_id"] = detection_id(nrt)
    unique = nrt.drop_duplicates(subset="detection_id").copy()
    rows_duplicate = len(nrt) - len(unique)

    unique["country_iso3"] = country_of(unique)
    clean = standardise(unique, batch_id)

    summary = {
        "batch_id": batch_id,
        "processed_at": processed_at,
        "rows_in": len(fires),
        "rows_rejected": len(rejected),
        "rows_clean": len(clean),
        "rows_filtered": rows_filtered,
        "rows_duplicate": rows_duplicate,
    }

    rows_accounted_for = len(clean) + len(rejected) + rows_filtered + rows_duplicate
    if rows_accounted_for != len(fires):
        raise ValueError(f"Row counts don't add up: {summary}")

    write_outputs(batch_id, clean, rejected, summary)
    print(f"Clean {len(clean)}, rejected {len(rejected)}, filtered {rows_filtered}, duplicate {rows_duplicate}")
    return summary


def main():
    parser = utils.make_parser("Combine, clean and standardise the day's raw FIRMS csvs")
    parser.add_argument("--raw-dir", type=Path, help="Local folder of raw files. Default: read from S3.")
    args = parser.parse_args()
    run(args.date, args.raw_dir)


if __name__ == "__main__":
    main()