import logging

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="demo_01_taskflow_api",
    description="Beginner demo: TaskFlow API with a simple ETL pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["demo", "taskflow"],
)
def taskflow_etl_demo():
    @task
    def extract() -> list:
        logging.info("Extracting raw data...")
        raw_data = [
            {"name": "  alice ", "score": "85"},
            {"name": "BOB", "score": "90"},
            {"name": "charlie", "score": "78"},
        ]
        logging.info(f"Extracted {len(raw_data)} records.")
        return raw_data

    @task
    def transform(raw_data: list) -> list:
        logging.info("Transforming data...")
        cleaned = []
        for record in raw_data:
            cleaned.append(
                {
                    "name": record["name"].strip().title(),
                    "score": int(record["score"]),
                    "grade": "Pass" if int(record["score"]) >= 80 else "Fail",
                }
            )
        logging.info(f"Transformed records: {cleaned}")
        return cleaned

    @task
    def load(clean_data: list) -> None:
        logging.info("Loading data to destination...")
        for record in clean_data:
            logging.info(
                f"  Saving: Name={record['name']}, "
                f"Score={record['score']}, Grade={record['grade']}"
            )
        logging.info("All records loaded successfully.")

    raw = extract()
    clean = transform(raw)
    load(clean)


taskflow_etl_demo()
