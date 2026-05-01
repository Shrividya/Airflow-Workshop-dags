"""
=============================================================
DEMO 3: Sensors
=============================================================

WHAT ARE SENSORS?
------------------
Sensors are a special type of Operator that WAIT for something
to happen before letting the pipeline continue.

Think of a sensor like a security guard at a door:
  - The guard checks every few seconds: "Is the package here yet?"
  - If YES → they open the door and let the pipeline through
  - If NO  → they wait and check again later (poke mode)
  - If it takes too long → they raise a timeout alarm

COMMON SENSORS:
  📁 FileSensor       → Wait for a file to appear
  🌐 HttpSensor       → Wait for an HTTP endpoint to return 200
  ⏰ TimeDeltaSensor  → Wait for a certain amount of time to pass
  🗄️ SqlSensor        → Wait for a SQL query to return rows

THIS DAG USES FileSensor:
  1. Sensor waits for a "trigger" file to appear in /tmp/airflow_demo/
  2. Once the file appears → pipeline continues
  3. You'll manually CREATE the file during the demo to unblock it!

HOW TO RUN THIS DEMO (step-by-step):
  1. Place this file in your dags/ folder
  2. Trigger the DAG — it will PAUSE at the sensor task (yellow spinner)
  3. Open a NEW terminal and run:
       docker compose exec airflow-worker touch /tmp/airflow_demo/trigger.txt
  4. Watch the sensor turn GREEN in the UI within ~15 seconds!
  5. The rest of the pipeline will then continue automatically.
=============================================================
"""

from airflow import DAG
# Airflow 3.0: use providers.standard.* imports (old airflow.* paths are deprecated)
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import logging


with DAG(
    dag_id="demo_03_sensors",
    description="Beginner demo: FileSensor — wait for a file before proceeding",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "sensors"],
) as dag:

    # ---------------------------------------------------
    # TASK 1: Setup
    # - Creates the watch directory
    # - Creates the fs_default connection if it doesn't exist
    #
    # WHY CREATE THE CONNECTION HERE?
    # In Airflow 3.0, direct ORM/database access from DAG code
    # is not allowed. The clean way to create connections is
    # via the Airflow CLI — which we call from a BashOperator.
    #
    # fs_default = "use the local filesystem, paths are absolute"
    # ---------------------------------------------------
    setup = BashOperator(
        task_id="setup_watch_directory",
        bash_command="""
            # Create the directory the sensor will watch
            mkdir -p /tmp/airflow_demo

            # Create fs_default connection if it doesn't already exist
            # 'airflow connections get' exits non-zero if not found, so we
            # use || to run 'add' only in that case
            airflow connections get fs_default > /dev/null 2>&1 \
                || airflow connections add fs_default \
                       --conn-type fs \
                       --conn-extra '{"path": "/"}'

            echo "==========================================="
            echo "  ✅ fs_default connection ready"
            echo "  👀 Watching: /tmp/airflow_demo/trigger.txt"
            echo ""
            echo "  To unblock this pipeline, open a NEW terminal and run:"
            echo "  docker compose exec airflow-worker touch /tmp/airflow_demo/trigger.txt"
            echo "==========================================="
        """,
    )

    # ---------------------------------------------------
    # TASK 2: FileSensor ← THE STAR OF THIS DEMO
    #
    # filepath      → what file to wait for (absolute path)
    # fs_conn_id    → which filesystem connection to use
    # poke_interval → check every N seconds
    # timeout       → give up and fail after N seconds
    # mode          → "poke"       = hold a worker slot while waiting
    #                 "reschedule" = release the slot between checks
    #                                (better for production / long waits)
    # ---------------------------------------------------
    wait_for_trigger_file = FileSensor(
        task_id="wait_for_trigger_file",
        filepath="/tmp/airflow_demo/trigger.txt",
        fs_conn_id="fs_default",
        poke_interval=15,    # Check every 15 seconds
        timeout=300,         # Timeout after 5 minutes
        mode="poke",
        soft_fail=False,     # Timeout = FAIL (not skip)
    )

    # ---------------------------------------------------
    # TASK 3: Confirm the file was found and log its details
    # ---------------------------------------------------
    def confirm_file(**context):
        import os
        path = "/tmp/airflow_demo/trigger.txt"
        if os.path.exists(path):
            size = os.path.getsize(path)
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            logging.info(f"✅ File confirmed: {path}")
            logging.info(f"   Size    : {size} bytes")
            logging.info(f"   Modified: {mtime}")
        else:
            logging.warning("⚠️ Sensor passed but file not found — unexpected state!")

    confirm_file_task = PythonOperator(
        task_id="confirm_file_found",
        python_callable=confirm_file,
    )

    # ---------------------------------------------------
    # TASK 4: Continue the pipeline after the sensor unblocks
    # ---------------------------------------------------
    process_after_trigger = BashOperator(
        task_id="process_after_trigger",
        bash_command="""
            echo "🚀 Trigger received! Starting downstream processing..."
            echo "   File contents:"
            cat /tmp/airflow_demo/trigger.txt || echo "   (file is empty — that's fine)"
            echo ""
            echo "✅ Pipeline unblocked and running successfully!"
        """,
    )

    # ---------------------------------------------------
    # TASK 5: Cleanup — remove the trigger file so the demo
    # can be re-run cleanly next time
    # ---------------------------------------------------
    cleanup = BashOperator(
        task_id="cleanup_trigger_file",
        bash_command="""
            rm -f /tmp/airflow_demo/trigger.txt
            echo "🧹 Trigger file removed. Ready for next run."
        """,
    )

    # ---------------------------------------------------
    # FLOW
    # ---------------------------------------------------
    setup >> wait_for_trigger_file >> confirm_file_task >> process_after_trigger >> cleanup
