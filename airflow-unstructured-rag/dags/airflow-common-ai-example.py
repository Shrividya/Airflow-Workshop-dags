from datetime import timedelta

from pydantic import BaseModel

from airflow.sdk import dag, task
from airflow.providers.common.ai.toolsets.sql import SQLToolset


class DailySummary(BaseModel):
    total_orders: int
    total_revenue: float
    notable_trends: list[str]


class FileFindings(BaseModel):
    anomalies: list[str]
    requires_investigation: bool


@dag(
    schedule="@daily",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def nightly_revenue_pipeline():

    @task.llm_schema_compare(
        llm_conn_id="pydanticai_default",
        db_conn_ids=["postgres_source", "snowflake_warehouse"],
        table_names=["orders", "customers"],
        context_strategy="full",
    )
    def check_schema_drift():
        return "Flag any mismatches that would break tonight's load. Report only, no migrations."


    comparison = check_schema_drift()

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "Given a schema comparison result, decide whether nightly ETL is safe to run. "
            "Route to 'generate_extract_query' if there are no critical mismatches, "
            "otherwise route to 'notify_team'."
        ),
    )
    def route_on_schema(comparison: dict):
        return f"Schema comparison result: {comparison}"

    route = route_on_schema(comparison)

    @task.llm_sql(
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_source",
        table_names=["orders", "customers"],
        dialect="postgres",
    )
    def generate_extract_query(ds=None):
        return f"Pull all orders and customer info for {ds}, joined on customer_id."

    @task
    def notify_team():
        import os
        import requests
        requests.post(
            os.environ["SLACK_WEBHOOK_URL"],
            json={"text": "Schema drift detected — halting ETL and notifying the data team."},
        )

    notify = notify_team()
    extract_query = generate_extract_query()
    route >> [notify, extract_query]

    @task.sql
    def run_extract_query(sql: str) -> list[dict]:
        return sql

    rows = run_extract_query(extract_query)

    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="Summarize the day's order data for a stakeholder digest.",
        output_type=DailySummary,
    )
    def summarize_orders(rows: list[dict]):
        return f"Summarize this order data: {rows}"

    summary = summarize_orders(rows)

    @task
    def export_report(summary: dict):
        path = "s3://reports/orders/daily_summary.json"
        print(f"Writing {summary} to {path}")
        return path

    report_path = export_report(summary)

    @task.llm_file_analysis(
        llm_conn_id="pydanticai_default",
        file_path=report_path,
        file_conn_id="aws_default",
        output_type=FileFindings,
    )
    def analyze_report():
        return "Review this report for any revenue anomalies or suspicious patterns."

    findings = analyze_report()

    @task.branch
    def needs_investigation(findings: dict):
        return "investigate_anomaly" if findings.get("requires_investigation") else "done"

    decision = needs_investigation(findings)

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a data analyst investigating a revenue anomaly. Use SQL to dig in.",
        toolsets=[
            SQLToolset(
                db_conn_id="postgres_source",
                allowed_tables=["orders", "customers"],
            )
        ],
    )
    def investigate_anomaly(findings: dict):
        return f"These anomalies were flagged: {findings}. Investigate the root cause."

    @task
    def done():
        print("No anomalies found -- pipeline complete.")

    decision >> [investigate_anomaly(findings), done()]


nightly_revenue_pipeline()
