
import argparse
import logging
import os
from datetime import date
from io import BytesIO

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)



EXPECTED_COLUMNS = {
    "latitude",
    "longitude",
    "bright_ti4",
    "scan",
    "track",
    "acq_date",
    "acq_time",
    "satellite",
    "instrument",
    "confidence",
    "version",
    "bright_ti5",
    "frp",
    "daynight",
}


def load_env_variables() -> tuple[str, str, str, str, str]:
    """
    Load AWS credentials and NASA FIRMS API key from .env.

    Returns:
        secret_key, access_key, map_key, bucket_name, bucket_region
    """
    load_dotenv()

    secret_key = os.getenv("SECRET_KEY")
    access_key = os.getenv("ACCESS_KEY")
    map_key = os.getenv("MAP_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_region = os.getenv("BUCKET_REGION")

    return (
        secret_key,
        access_key,
        map_key,
        bucket_name,
        bucket_region,
    )



def arg_parse() -> date:
    """
    Parse the processing date supplied via CLI.

    Example:
        python extract.py --date 2026-10-18
    """
    parser = argparse.ArgumentParser(
        description="NASA FIRMS extraction pipeline"
    )

    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="Processing date in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    return args.date



def extract(
    map_key: str,
    processing_date: date,
) -> pd.DataFrame:
    """
    Extract one day of global VIIRS NOAA-20 NRT fire data.

    Raises:
        requests.exceptions.RequestException: API request failure.
        pd.errors.ParserError: CSV parsing failure.

    Returns:
        pd.DataFrame: Extracted fire detection records.
    """
    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{map_key}/VIIRS_NOAA20_NRT/world/1/"
        f"{processing_date.isoformat()}"
    )

    logger.info("Extracting FIRMS data for %s", processing_date)

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        df = pd.read_csv(BytesIO(response.content))

        logger.info("Extracted %d records", len(df))
        return df

    except requests.exceptions.RequestException as e:
        logger.error("NASA FIRMS API request failed: %s", e)
        raise

    except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        logger.error("Failed to parse FIRMS CSV: %s", e)
        raise


def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the expected NASA FIRMS column names.

    Raises ValueError if columns are missing or unexpected.
    Does not modify the DataFrame.
    """
    actual_columns = set(df.columns)

    missing = EXPECTED_COLUMNS - actual_columns
    unexpected = actual_columns - EXPECTED_COLUMNS

    if missing or unexpected:
        raise ValueError(
            f"Schema mismatch. Missing: {sorted(missing)}, "
            f"Unexpected: {sorted(unexpected)}"
        )

    logger.info(
        "Schema validation passed: %d columns",
        len(df.columns),
    )
    logger.info("df.info()")
    return df



def write_df_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> None:
    """
    Convert a DataFrame to CSV in memory and upload to S3.

    Uses BytesIO to avoid creating a local CSV file.
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    buffer = BytesIO()

    df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3.upload_fileobj(
        Fileobj=buffer,
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": "text/csv"},
    )

    logger.info(
        "Uploaded %d records to s3://%s/%s",
        len(df),
        bucket,
        key,
    )




def main() -> None:
    """Run extraction, schema validation, and S3 upload."""
    processing_date = arg_parse()

    (
        secret_key,
        access_key,
        map_key,
        bucket_name,
        bucket_region,
    ) = load_env_variables()

    # Extract raw data
    df = extract(map_key, processing_date)

    # Validate expected column names
    df = validate_schema(df)

    # Partition S3 objects by processing date
    s3_key = (
        f"raw/date={processing_date.isoformat()}/"
        f"viirs_noaa20_nrt.csv"
    )

    # Upload CSV to S3
    write_df_to_s3(
        df=df,
        bucket=bucket_name,
        key=s3_key,
        access_key=access_key,
        secret_key=secret_key,
        region=bucket_region,
    )

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
