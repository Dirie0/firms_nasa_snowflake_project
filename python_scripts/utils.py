import argparse
import os
from datetime import date
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def make_parser(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="Processing date, YYYY-MM-DD")
    return parser


def get_env(name):
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing environment variable: {name} (see .env.example)")
    return value


def s3():
    return boto3.client("s3")

def s3_get(key):
    return s3().get_object(Bucket=get_env("BUCKET_NAME"), Key=key)["Body"].read()


def s3_put(key, body):
    s3().put_object(Bucket=get_env("BUCKET_NAME"), Key=key, Body=body)


def s3_delete(key):
    s3().delete_object(Bucket=get_env("BUCKET_NAME"), Key=key)


def s3_put_csv(key, frame):
    s3_put(key, frame.to_csv(index=False, lineterminator="\n").encode("utf-8"))


def read_csv_as_text(source):
    return pd.read_csv(BytesIO(source), dtype=str, keep_default_na=False)