import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="demo_04_hooks",
    description="Beginner demo: HttpHook -- reusable connections to external systems",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["demo", "hooks"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
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

    def fetch_post(**context):
        from airflow.providers.http.hooks.http import HttpHook

        logging.info("Creating HttpHook with connection: jsonplaceholder_api")
        hook = HttpHook(method="GET", http_conn_id="jsonplaceholder_api")

        logging.info("Fetching post #42 from API...")
        response = hook.run(endpoint="/posts/42")

        post = response.json()
        logging.info("Got post:")
        logging.info(f"   ID    : {post['id']}")
        logging.info(f"   Title : {post['title']}")
        logging.info(f"   Body  : {post['body'][:80]}...")

        context["ti"].xcom_push(key="post_user_id", value=post["userId"])
        context["ti"].xcom_push(key="post_title", value=post["title"])
        return post

    fetch_post_task = PythonOperator(
        task_id="fetch_post",
        python_callable=fetch_post,
    )

    def fetch_post_author(**context):
        from airflow.providers.http.hooks.http import HttpHook

        user_id = context["ti"].xcom_pull(task_ids="fetch_post", key="post_user_id")
        post_title = context["ti"].xcom_pull(task_ids="fetch_post", key="post_title")

        logging.info(
            f"Fetching author (user_id={user_id}) for post: '{post_title[:40]}...'"
        )

        hook = HttpHook(method="GET", http_conn_id="jsonplaceholder_api")
        response = hook.run(endpoint=f"/users/{user_id}")

        user = response.json()
        logging.info("Author details:")
        logging.info(f"   Name    : {user['name']}")
        logging.info(f"   Email   : {user['email']}")
        logging.info(f"   Company : {user['company']['name']}")
        logging.info(f"   City    : {user['address']['city']}")

    fetch_author_task = PythonOperator(
        task_id="fetch_post_author",
        python_callable=fetch_post_author,
    )

    def summarize(**context):
        post_title = context["ti"].xcom_pull(task_ids="fetch_post", key="post_title")
        logging.info("=" * 55)
        logging.info("  HOOKS DEMO COMPLETE")
        logging.info("=" * 55)
        logging.info("  Used HttpHook with a named connection (not hardcoded)")
        logging.info("  Fetched a post from the API")
        logging.info("  Reused the same Hook to fetch the author")
        logging.info(f"  Post title: '{post_title}'")
        logging.info("")
        logging.info("  KEY INSIGHT:")
        logging.info("  If the API URL or credentials change, you update")
        logging.info("  ONE place (Admin -> Connections) -- not every DAG!")
        logging.info("=" * 55)

    summary_task = PythonOperator(
        task_id="summary",
        python_callable=summarize,
    )

    intro >> fetch_post_task >> fetch_author_task >> summary_task
