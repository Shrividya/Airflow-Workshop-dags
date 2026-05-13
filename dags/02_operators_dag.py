import logging

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="demo_02_operators",
    description="Beginner demo: BashOperator, PythonOperator, BranchPythonOperator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["demo", "operators"],
    params={
        "student_score": Param(85, type="integer", description="Score to evaluate")
    },
) as dag:
    print_system_info = BashOperator(
        task_id="print_system_info",
        bash_command="""
            echo "==========================================="
            echo "  Airflow Operators Demo Starting!"
            echo "  Container time: $(date)"
            echo "  Python version: $(python3 --version)"
            echo "==========================================="
        """,
    )

    def process_student(**context):
        score = context["params"]["student_score"]
        logging.info(f"Processing student score: {score}")
        logging.info(f"  DAG run ID: {context['run_id']}")
        context["ti"].xcom_push(key="student_score", value=score)
        logging.info("  Score pushed to XCom")

    process_student_task = PythonOperator(
        task_id="process_student",
        python_callable=process_student,
    )

    def decide_path(**context):
        score = context["ti"].xcom_pull(task_ids="process_student", key="student_score")
        logging.info(f"Deciding path for score: {score}")
        if score >= 80:
            logging.info("  Taking HIGH SCORE path")
            return "high_score_path"
        else:
            logging.info("  Taking LOW SCORE path")
            return "low_score_path"

    branch_task = BranchPythonOperator(
        task_id="check_score_branch",
        python_callable=decide_path,
    )

    high_score_path = BashOperator(
        task_id="high_score_path",
        bash_command='echo "Great job! Score >= 80. Sending congratulations email..."',
    )

    low_score_path = BashOperator(
        task_id="low_score_path",
        bash_command='echo "Score < 80. Scheduling a revision session..."',
    )

    end = BashOperator(
        task_id="pipeline_complete",
        bash_command='echo "Pipeline finished. Branch result recorded."',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        print_system_info
        >> process_student_task
        >> branch_task
        >> [high_score_path, low_score_path]
        >> end
    )
