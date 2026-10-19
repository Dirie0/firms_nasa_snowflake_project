
import argparse
import logging
import os
from datetime import date
from io import BytesIO

import boto3
import great_expectations as gx
import pandas as pd
from dotenv import load_dotenv


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Environment variables
# --------------------------------------------------

def load_env_variables() -> tuple[
    str | None, str | None, str | None, str | None
]:
    """
    Load AWS credentials and S3 configuration from .env.

    Returns:
        secret_key, access_key, bucket_name, bucket_region
    """
    load_dotenv()

    secret_key = os.getenv("SECRET_KEY")
    access_key = os.getenv("ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_region = os.getenv("BUCKET_REGION")

    return secret_key, access_key, bucket_name, bucket_region


# --------------------------------------------------
# CLI argument parsing
# --------------------------------------------------

def arg_parse() -> date:
    """
    Parse the processing date supplied via CLI.

    Example:
        python transform.py --date 2026-10-18
    """
    parser = argparse.ArgumentParser(
        description="NASA FIRMS transformation pipeline"
    )

    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="Processing date in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    return args.date


# --------------------------------------------------
# Read raw CSV from S3
# --------------------------------------------------

def read_df_from_s3(
    bucket: str,
    key: str,
    access_key: str | None,
    secret_key: str | None,
    region: str | None,
) -> pd.DataFrame:
    """
    Read a raw NASA FIRMS CSV from S3 into a Pandas DataFrame.
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    logger.info("Reading s3://%s/%s", bucket, key)

    response = s3.get_object(
        Bucket=bucket,
        Key=key,
    )

    df = pd.read_csv(BytesIO(response["Body"].read()))

    logger.info(
        "Loaded %d rows and %d columns",
        len(df),
        len(df.columns),
    )

    return df


# --------------------------------------------------
# Great Expectations validation
# --------------------------------------------------

def validate_data(df: pd.DataFrame) -> None:
    """
    Validate NASA FIRMS detections using Great Expectations.

    Raise ValueError if any expectation fails.
    """
    context = gx.get_context(mode="ephemeral")

    # Register Pandas DataFrame
    data_source = context.data_sources.add_pandas(
        name="nasa_firms"
    )

    data_asset = data_source.add_dataframe_asset(
        name="fire_detections"
    )

    batch_definition = (
        data_asset.add_batch_definition_whole_dataframe(
            "daily_fire_detections"
        )
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    # Define expectation suite
    suite = gx.ExpectationSuite(
        name="firms_validation_suite"
    )

    expected_columns = [
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
    ]

    # Schema validation
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=expected_columns,
            exact_match=False,
        )
    )

    # Ensure DataFrame is not empty
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1
        )
    )

    # Coordinate validation
    for column, minimum, maximum in [
        ("latitude", -90, 90),
        ("longitude", -180, 180),
    ]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=column,
                min_value=minimum,
                max_value=maximum,
            )
        )

    # Required fields
    for column in [
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
    ]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column
            )
        )

    # Confidence categories
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="confidence",
            value_set=["l", "n", "h"],
        )
    )

    # Day/night categories
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="daynight",
            value_set=["D", "N"],
        )
    )

    # Run validation
    results = batch.validate(suite)

    for result in results.results:
        logger.info(
            "Expectation: %s | Passed: %s",
            result.expectation_config.type,
            result.success,
        )

    if not results.success:
        raise ValueError(
            "NASA FIRMS data validation failed"
        )

    logger.info(
        "All Great Expectations checks passed"
    )



def main() -> None:
    """
    Read and validate raw NASA FIRMS data from S3
    for a specified processing date.
    """
    processing_date = arg_parse()

    (
        secret_key,
        access_key,
        bucket_name,
        bucket_region,
    ) = load_env_variables()

    # Construct S3 key
    s3_key = (
        f"raw/date={processing_date.isoformat()}/"
        f"viirs_noaa20_nrt.csv"
    )

    # Read raw CSV from S3
    df = read_df_from_s3(
        bucket=bucket_name,
        key=s3_key,
        access_key=access_key,
        secret_key=secret_key,
        region=bucket_region,
    )

    # Validate raw data
    validate_data(df)

    # Transformations will be added here
    # df = transform(df)

    logger.info(
        "NASA FIRMS validation completed successfully"
    )


if __name__ == "__main__":
    main()
