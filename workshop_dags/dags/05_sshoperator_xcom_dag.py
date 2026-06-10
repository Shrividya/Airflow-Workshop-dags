import base64
import json
import logging
from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag, task


@dag(
    dag_id="ssh_taskflow_etl",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["shri"],
)
def ssh_taskflow_etl():

    @task
    def build_command(ds: str) -> str:
        return f"python3 /opt/scripts/etl.py --date {ds} --env dev"

    @task
    def parse_result(raw: bytes) -> dict:
        decoded = base64.b64decode(raw).decode("utf-8").strip()
        result = json.loads(decoded)
        logging.info(f"ETL complete: {result['rows_processed']} rows processed")
        return result

    @task
    def validate_results(result: dict) -> str:
        failed = result.get("failed_rows")
        path = result.get("output_path")

        if failed is None or path is None:
            raise ValueError(f"Unexpected ETL output shape: {result}")
        if failed > 100:
            raise ValueError(f"Too many failures: {failed}")

        logging.info(f"Validated. Output at: {path}")
        return path

    @task
    def trigger_downstream(output_path: str):
        logging.info(f"Notifying downstream systems about: {output_path}")

    command = build_command()

    run_remote = SSHOperator(
        task_id="run_remote_etl",
        ssh_conn_id="my_remote_server",
        command=command,
        do_xcom_push=True,
        get_pty=False,  # must be False for clean stdout/XCom
        cmd_timeout=600,
    )

    parsed = parse_result(run_remote.output)
    path = validate_results(parsed)
    trigger_downstream(path)


ssh_taskflow_etl()
