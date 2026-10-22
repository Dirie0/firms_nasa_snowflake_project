import json

import pandas as pd

import config
import utils


def run(processing_date):
    batch_id = processing_date.isoformat()
    source = config.TRANSFORMED_PREFIX.format(date=batch_id)
    target = config.TABLES_PREFIX.format(date=batch_id)

    clean = utils.read_csv_as_text(utils.s3_get(source + "clean.csv"))
    rejected = utils.read_csv_as_text(utils.s3_get(source + "rejected.csv"))
    summary = json.loads(utils.s3_get(source + "run_summary.json"))
    run_summary = pd.DataFrame([summary])[config.RUN_SUMMARY_COLUMNS]

    utils.s3_put_csv(target + "fact_fire_detection.csv", clean[config.FACT_COLUMNS])
    utils.s3_put_csv(target + "rejected_detections.csv", rejected[config.REJECTED_COLUMNS])
    utils.s3_put_csv(target + "run_summary.csv", run_summary)

    print(f"Wrote fact ({len(clean)}), rejected ({len(rejected)}) and run_summary to s3://.../{target}")


def main():
    args = utils.make_parser("Split transformed data into the fact and rejected tables").parse_args()
    run(args.date)


if __name__ == "__main__":
    main()
