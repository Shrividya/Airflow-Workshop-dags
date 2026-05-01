"""
=============================================================
DEMO 2: Operators
=============================================================

WHAT ARE OPERATORS?
--------------------
Operators are the original building blocks of Airflow.
Think of them as LEGO bricks — each one does a specific job:

  🔧 BashOperator       → Runs a shell command (like typing in a terminal)
  🐍 PythonOperator     → Runs a Python function
  🔀 BranchPythonOperator → Makes a decision (go left or go right?)
  📧 EmailOperator      → Sends an email (not demoed here — needs SMTP)
  ... and hundreds more for databases, cloud, etc.

THIS DAG DEMONSTRATES:
  1. BashOperator    — print system info
  2. PythonOperator  — process some data
  3. BranchPythonOperator — route to "high score" or "low score" path
  4. Downstream tasks depending on the branch decision

HOW TO RUN:
  1. Place this file in your dags/ folder
  2. Open Airflow UI → find "demo_02_operators"
  3. Trigger it — in the Graph view, watch which branch gets picked!
  4. Try editing STUDENT_SCORE below and re-trigger to flip the branch.
=============================================================
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

# -------------------------------------------------------
# Adjust this score to control which branch runs!
# ≥ 80 → "high_score_path", < 80 → "low_score_path"
# -------------------------------------------------------
STUDENT_SCORE = 85

# -------------------------------------------------------
# DAG DEFINITION (classic style — no @dag decorator)
# -------------------------------------------------------
with DAG(
    dag_id="demo_02_operators",
    description="Beginner demo: BashOperator, PythonOperator, BranchPythonOperator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "operators"],
) as dag:

    # ---------------------------------------------------
    # TASK 1: BashOperator
    # Runs a shell command inside the Airflow container.
    # Great for: running scripts, calling CLI tools, echoing info.
    # ---------------------------------------------------
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

    # ---------------------------------------------------
    # TASK 2: PythonOperator
    # Runs a plain Python function. The function can do
    # anything: API calls, data processing, file I/O, etc.
    # ---------------------------------------------------
    def process_student(**context):
        """
        **context is a special Airflow dict that contains
        useful info like the run date, DAG ID, etc.
        We can also use it to push data via XCom.
        """
        score = STUDENT_SCORE
        logging.info(f"🎓 Processing student score: {score}")
        logging.info(f"  DAG run ID: {context['run_id']}")

        # Push the score to XCom so the next task can read it
        # XCom = "cross-communication" — Airflow's message-passing system
        context["ti"].xcom_push(key="student_score", value=score)
        logging.info("  Score pushed to XCom ✓")

    process_student_task = PythonOperator(
        task_id="process_student",
        python_callable=process_student,
    )

    # ---------------------------------------------------
    # TASK 3: BranchPythonOperator
    # The DECISION MAKER. Returns the task_id of the next
    # task to run. All other branches are SKIPPED.
    #
    # Think of it as a railway switch — only one track gets
    # the train, the rest stay empty.
    # ---------------------------------------------------
    def decide_path(**context):
        # Pull the score we saved earlier from XCom
        score = context["ti"].xcom_pull(task_ids="process_student", key="student_score")
        logging.info(f"🔀 Deciding path for score: {score}")

        if score >= 80:
            logging.info("  → Taking HIGH SCORE path")
            return "high_score_path"   # Return the task_id to run
        else:
            logging.info("  → Taking LOW SCORE path")
            return "low_score_path"

    branch_task = BranchPythonOperator(
        task_id="check_score_branch",
        python_callable=decide_path,
    )

    # ---------------------------------------------------
    # TASK 4a: High score branch
    # ---------------------------------------------------
    high_score_path = BashOperator(
        task_id="high_score_path",
        bash_command='echo "🏆 Great job! Score ≥ 80. Sending congratulations email..."',
    )

    # ---------------------------------------------------
    # TASK 4b: Low score branch
    # ---------------------------------------------------
    low_score_path = BashOperator(
        task_id="low_score_path",
        bash_command='echo "📚 Score < 80. Scheduling a revision session..."',
    )

    # ---------------------------------------------------
    # TASK 5: Join — runs regardless of which branch was taken
    # trigger_rule="none_failed_min_one_success" means:
    # "run me as long as at least one upstream succeeded"
    # (default rule would skip this since one branch is skipped)
    # ---------------------------------------------------
    from airflow.utils.trigger_rule import TriggerRule

    end = BashOperator(
        task_id="pipeline_complete",
        bash_command='echo "✅ Pipeline finished! Branch result recorded."',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------------------------------------------------
    # DEFINE THE FLOW (dependencies)
    # >> means "runs before"
    # [a, b] means "both a and b come before the next task"
    # ---------------------------------------------------
    (
        print_system_info
        >> process_student_task
        >> branch_task
        >> [high_score_path, low_score_path]
        >> end
    )
