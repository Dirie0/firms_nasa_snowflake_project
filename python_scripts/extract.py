from io import BytesIO

import pandas as pd
import requests

import config
import utils


def download_fires(satellite, processing_date, map_key):
    name = satellite["satellite_name"]
    url = config.FIRMS_URL.format(
        map_key=map_key,
        source=satellite["source"],
        date=processing_date.isoformat(),
    )
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        return pd.read_csv(BytesIO(response.content))
    except requests.HTTPError as error:
        print(f"{name}: NASA returned HTTP {error.response.status_code}")
    except requests.RequestException as error:
        print(f"{name}: request failed ({type(error).__name__})")
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        print(f"{name}: response was not a valid csv")
    return None


def main():
    parser = utils.make_parser("Download raw NASA FIRMS data to S3")
    parser.add_argument("--satellite", choices=list(config.SATELLITES), help="One satellite. Default: all.")
    args = parser.parse_args()

    map_key = utils.get_env("MAP_KEY")
    satellites_to_process = [args.satellite] if args.satellite else list(config.SATELLITES)
    failed = []

    for s in satellites_to_process:
        satellite = config.SATELLITES[s]
        fires = download_fires(satellite, args.date, map_key)
        if fires is None:
            failed.append(satellite["satellite_name"])
            continue
        key = config.RAW_KEY.format(date=args.date.isoformat(), code=s.lower())
        utils.s3_put(key, fires.to_csv(index=False).encode("utf-8"))
        print(f"{satellite['satellite_name']}: {len(fires)} fires -> s3://.../{key}")

    if failed:
        raise SystemExit(f"Download failed for: {', '.join(failed)}")


if __name__ == "__main__":
    main()