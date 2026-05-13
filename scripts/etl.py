import argparse
import json
import sys
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(description="Run ETL pipeline for a given date.")
    parser.add_argument(
        "--date", required=True, help="Processing date in YYYY-MM-DD format"
    )
    parser.add_argument("--env", default="prod", help="Target environment")
    return parser.parse_args()


def validate_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD.")
    return date_str


def main():
    args = parse_args()
    validate_date(args.date)

    # All logging -> stderr (keeps stdout clean for XCom)
    print(f"Running ETL for {args.date}", file=sys.stderr)

    result = {
        "rows_processed": 42000,
        "failed_rows": 3,
        "output_path": f"s3://my-bucket/output/{args.date}/",
        "status": "success",
    }

    print(json.dumps(result))


if __name__ == "__main__":
    main()
