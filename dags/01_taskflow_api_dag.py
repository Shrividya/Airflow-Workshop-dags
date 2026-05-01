"""
=============================================================
DEMO 1: TaskFlow API
=============================================================

WHAT IS THE TASKFLOW API?
--------------------------
Think of a traditional recipe vs. a modern cooking app.

  Old way (classic Airflow): You write each step separately, and
  you manually pass ingredients (data) between steps using
  something called XCom — like passing notes between cooks.

  New way (TaskFlow API): You just write Python functions and
  decorate them with @task. Airflow figures out the flow and
  passes data automatically. Much cleaner!

THIS DAG SIMULATES A SIMPLE ETL PIPELINE:
  Extract → Transform → Load
  (like reading a CSV, cleaning it, then saving to a database)

HOW TO RUN:
  1. Place this file in your dags/ folder
  2. Open Airflow UI at http://localhost:8080
  3. Find "demo_01_taskflow_api" and toggle it ON
  4. Click the ▶ (Trigger DAG) button
  5. Watch the Graph view to see tasks run in order
=============================================================
"""

from airflow.decorators import dag, task
from datetime import datetime
import logging

# The @dag decorator turns this function into an Airflow DAG
@dag(
    dag_id="demo_01_taskflow_api",
    description="Beginner demo: TaskFlow API with a simple ETL pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # None = only run when triggered manually
    catchup=False,          # Don't run for past dates we missed
    tags=["demo", "taskflow"],
)
def taskflow_etl_demo():
    """
    A simple Extract → Transform → Load pipeline using the TaskFlow API.
    Each step is just a Python function with @task on top!
    """

    # -------------------------------------------------------
    # STEP 1: EXTRACT
    # Normally this would read from a file, database, or API.
    # Here we simulate it by returning a list of raw records.
    # -------------------------------------------------------
    @task
    def extract() -> list:
        logging.info("📥 Extracting raw data...")

        # Simulated raw data (imagine this came from a CSV or API)
        raw_data = [
            {"name": "  alice ", "score": "85"},
            {"name": "BOB",      "score": "90"},
            {"name": "charlie",  "score": "78"},
        ]

        logging.info(f"Extracted {len(raw_data)} records.")
        return raw_data   # TaskFlow automatically passes this to the next task

    # -------------------------------------------------------
    # STEP 2: TRANSFORM
    # Clean and standardize the data before storing it.
    # -------------------------------------------------------
    @task
    def transform(raw_data: list) -> list:
        logging.info("🔄 Transforming data...")

        cleaned = []
        for record in raw_data:
            cleaned.append({
                "name": record["name"].strip().title(),  # "  alice " → "Alice"
                "score": int(record["score"]),            # "85" (string) → 85 (int)
                "grade": "Pass" if int(record["score"]) >= 80 else "Fail",
            })

        logging.info(f"Transformed records: {cleaned}")
        return cleaned

    # -------------------------------------------------------
    # STEP 3: LOAD
    # Normally this saves to a database. Here we just log it.
    # -------------------------------------------------------
    @task
    def load(clean_data: list) -> None:
        logging.info("💾 Loading data to destination...")

        for record in clean_data:
            logging.info(
                f"  Saving → Name: {record['name']}, "
                f"Score: {record['score']}, Grade: {record['grade']}"
            )

        logging.info("✅ All records loaded successfully!")

    # -------------------------------------------------------
    # WIRE THE TASKS TOGETHER
    # This is where the magic happens — just call them like
    # regular Python functions. Airflow builds the dependency
    # graph automatically from how you chain the calls.
    # -------------------------------------------------------
    raw = extract()
    clean = transform(raw)
    load(clean)


# This line actually registers the DAG with Airflow
taskflow_etl_demo()
