import logging
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


with DAG(
    dag_id="demo_03_sensors",
    description="Beginner demo: FileSensor -- wait for a file before proceeding",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["demo", "sensors"],
) as dag:
    setup = BashOperator(
        task_id="setup_watch_directory",
        bash_command="""
            mkdir -p /tmp/airflow_demo

            airflow connections get fs_default > /dev/null 2>&1 \
                || airflow connections add fs_default \
                       --conn-type fs \
                       --conn-extra '{"path": "/"}'

            echo "==========================================="
            echo "  fs_default connection ready"
            echo "  Watching: /tmp/airflow_demo/trigger.txt"
            echo ""
            echo "  To unblock this pipeline, open a NEW terminal and run:"
            echo "  docker compose exec airflow-worker touch /tmp/airflow_demo/trigger.txt"
            echo "==========================================="
        """,
    )

    wait_for_trigger_file = FileSensor(
        task_id="wait_for_trigger_file",
        filepath="/tmp/airflow_demo/trigger.txt",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=300,
        mode="poke",
        soft_fail=False,
    )

    def confirm_file(**context):
        import os

        path = "/tmp/airflow_demo/trigger.txt"
        if os.path.exists(path):
            size = os.path.getsize(path)
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            logging.info(f"File confirmed: {path}")
            logging.info(f"   Size    : {size} bytes")
            logging.info(f"   Modified: {mtime}")
        else:
            logging.warning("Sensor passed but file not found -- unexpected state!")

    confirm_file_task = PythonOperator(
        task_id="confirm_file_found",
        python_callable=confirm_file,
    )

    process_after_trigger = BashOperator(
        task_id="process_after_trigger",
        bash_command="""
            echo "Trigger received. Starting downstream processing..."
            echo "   File contents:"
            cat /tmp/airflow_demo/trigger.txt || echo "   (file is empty -- that's fine)"
            echo ""
            echo "Pipeline unblocked and running successfully."
        """,
    )

    cleanup = BashOperator(
        task_id="cleanup_trigger_file",
        bash_command="""
            rm -f /tmp/airflow_demo/trigger.txt
            echo "Trigger file removed. Ready for next run."
        """,
    )

    (
        setup
        >> wait_for_trigger_file
        >> confirm_file_task
        >> process_after_trigger
        >> cleanup
    )
