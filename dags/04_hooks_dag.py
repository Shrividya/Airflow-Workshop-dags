"""
=============================================================
DEMO 4: Hooks
=============================================================

WHAT ARE HOOKS?
----------------
Hooks are Airflow's way of managing CONNECTIONS to external systems.

Think of a Hook like a power adapter:
  - The adapter (Hook) knows HOW to plug into a specific socket
    (database, API, cloud service)
  - You don't rewrite the connection logic every time — you just
    grab the right adapter and use it
  - Connection details (host, password, port) are stored securely
    in Airflow's Connections UI, NOT hardcoded in your DAG

COMMON HOOKS:
  🌐 HttpHook      → Talk to REST APIs
  🐘 PostgresHook  → Connect to PostgreSQL
  ☁️ S3Hook        → Read/write from AWS S3
  🔑 BaseHook      → Base class all hooks inherit from

THIS DAG USES HttpHook:
  - Calls a free public API: https://jsonplaceholder.typicode.com
    (it's like a fake API for testing — always available, no auth)
  - Fetches a "post" and a "user" to simulate a real data pull
  - Shows how Hooks keep connection config OUT of your code

SETUP REQUIRED (one-time):
  1. Open Airflow UI → Admin → Connections
  2. Click the + button to add a new connection
  3. Fill in:
       Connection Id : jsonplaceholder_api
       Connection Type: HTTP
       Host          : https://jsonplaceholder.typicode.com
  4. Click Save
  5. Now trigger this DAG!
=============================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import logging
import json

with DAG(
    dag_id="demo_04_hooks",
    description="Beginner demo: HttpHook — reusable connections to external systems",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "hooks"],
) as dag:

    # ---------------------------------------------------
    # TASK 1: Explain what we're about to do
    # ---------------------------------------------------
    intro = BashOperator(
        task_id="intro",
        bash_command="""
            echo "============================================="
            echo "  Hooks Demo: Calling an external HTTP API"
            echo "  API: https://jsonplaceholder.typicode.com"
            echo "  (A free fake REST API for testing)"
            echo "============================================="
        """,
    )

    # ---------------------------------------------------
    # TASK 2: Fetch a post using HttpHook
    #
    # HttpHook takes the connection_id we set up in the UI.
    # It handles: base URL, headers, auth tokens — all stored
    # securely in Airflow, NOT in your code.
    # ---------------------------------------------------
    def fetch_post(**context):
        from airflow.providers.http.hooks.http import HttpHook

        logging.info("🌐 Creating HttpHook with connection: jsonplaceholder_api")

        # This is the Hook — it reads connection details from Airflow's vault
        hook = HttpHook(
            method="GET",
            http_conn_id="jsonplaceholder_api",  # Connection we set up in UI
        )

        # Make the API call — just like requests.get() but managed by Airflow
        logging.info("📡 Fetching post #42 from API...")
        response = hook.run(endpoint="/posts/42")

        post = response.json()
        logging.info(f"✅ Got post:")
        logging.info(f"   ID    : {post['id']}")
        logging.info(f"   Title : {post['title']}")
        logging.info(f"   Body  : {post['body'][:80]}...")

        # Push to XCom so the next task can use it
        context["ti"].xcom_push(key="post_user_id", value=post["userId"])
        context["ti"].xcom_push(key="post_title", value=post["title"])

        return post  # TaskFlow would do this automatically, but here we use xcom_push

    fetch_post_task = PythonOperator(
        task_id="fetch_post",
        python_callable=fetch_post,
    )

    # ---------------------------------------------------
    # TASK 3: Fetch the author of that post using the same Hook
    # This shows reusability — same connection, different endpoint
    # ---------------------------------------------------
    def fetch_post_author(**context):
        from airflow.providers.http.hooks.http import HttpHook

        # Pull the user_id we saved from the previous task
        user_id = context["ti"].xcom_pull(task_ids="fetch_post", key="post_user_id")
        post_title = context["ti"].xcom_pull(task_ids="fetch_post", key="post_title")

        logging.info(f"👤 Fetching author (user_id={user_id}) for post: '{post_title[:40]}...'")

        # Same hook, DIFFERENT endpoint — reusability at work!
        hook = HttpHook(method="GET", http_conn_id="jsonplaceholder_api")
        response = hook.run(endpoint=f"/users/{user_id}")

        user = response.json()
        logging.info(f"✅ Author details:")
        logging.info(f"   Name    : {user['name']}")
        logging.info(f"   Email   : {user['email']}")
        logging.info(f"   Company : {user['company']['name']}")
        logging.info(f"   City    : {user['address']['city']}")

    fetch_author_task = PythonOperator(
        task_id="fetch_post_author",
        python_callable=fetch_post_author,
    )

    # ---------------------------------------------------
    # TASK 4: Summary — show what we demonstrated
    # ---------------------------------------------------
    def summarize(**context):
        post_title = context["ti"].xcom_pull(task_ids="fetch_post", key="post_title")

        logging.info("=" * 55)
        logging.info("  HOOKS DEMO COMPLETE")
        logging.info("=" * 55)
        logging.info("  ✅ Used HttpHook with a named connection (not hardcoded)")
        logging.info("  ✅ Fetched a post from the API")
        logging.info("  ✅ Reused the same Hook to fetch the author")
        logging.info(f"  ✅ Post title: '{post_title}'")
        logging.info("")
        logging.info("  KEY INSIGHT:")
        logging.info("  If the API URL or credentials change, you update")
        logging.info("  ONE place (Admin → Connections) — not every DAG!")
        logging.info("=" * 55)

    summary_task = PythonOperator(
        task_id="summary",
        python_callable=summarize,
    )

    # ---------------------------------------------------
    # FLOW
    # ---------------------------------------------------
    intro >> fetch_post_task >> fetch_author_task >> summary_task
