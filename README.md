# Airflow Workshop DAGs

A hands-on collection of Apache Airflow 3 DAGs demonstrating core concepts — TaskFlow API, Operators, Sensors, Hooks, and SSHOperator with XCom — designed as a progressive workshop curriculum.

## DAGs Overview

| # | DAG ID | Concepts Covered |
|---|--------|-----------------|
| 01 | `demo_01_taskflow_api` | TaskFlow API, `@task` decorator, ETL pipeline |
| 02 | `demo_02_operators` | BashOperator, PythonOperator, BranchPythonOperator, XCom, TriggerRule |
| 03 | `demo_03_sensors` | FileSensor, poke mode, connections |
| 04 | `demo_04_hooks` | HttpHook, named connections, XCom chaining |
| 05 | `ssh_taskflow_etl` | SSHOperator, XCom with remote output, TaskFlow + classic operator mix |

---

## DAG Details

### 01 — TaskFlow API ETL (`demo_01_taskflow_api`)

Introduces the modern TaskFlow API using `@dag` and `@task` decorators to build a simple ETL pipeline.

- **Extract**: returns a list of raw student records
- **Transform**: cleans names, casts scores, assigns Pass/Fail grades
- **Load**: logs each cleaned record to simulate a write

```
extract → transform → load
```

---

### 02 — Operators (`demo_02_operators`)

Demonstrates the classic DAG authoring style with three operator types and branching logic.

- `BashOperator` prints system info
- `PythonOperator` reads a `student_score` DAG param and pushes it to XCom
- `BranchPythonOperator` routes to a **high score** or **low score** path
- Final task uses `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` to converge both branches

```
print_system_info → process_student → check_score_branch → high_score_path ─┐
                                                          └→ low_score_path  ─┤
                                                                              └→ pipeline_complete
```

**Param:** `student_score` (integer, default `85`) — set at trigger time via the Airflow UI.

---

### 03 — Sensors (`demo_03_sensors`)

Shows how to pause a pipeline until an external condition is met using `FileSensor`.

- Creates `/tmp/airflow_demo/` and registers the `fs_default` connection if missing
- `FileSensor` polls every 15 s (timeout 300 s) for `/tmp/airflow_demo/trigger.txt`
- Once the file appears: confirms metadata, processes downstream, then cleans up

```
setup_watch_directory → wait_for_trigger_file → confirm_file_found → process_after_trigger → cleanup_trigger_file
```

**To unblock the sensor:**
```bash
docker compose exec airflow-worker touch /tmp/airflow_demo/trigger.txt
```

---

### 04 — Hooks (`demo_04_hooks`)

Illustrates how `HttpHook` abstracts connection details away from DAG code.

- Connects to `jsonplaceholder.typicode.com` via the `jsonplaceholder_api` Airflow connection
- Fetches post #42 and pushes `userId` and `title` to XCom
- Reuses the same hook to fetch the post's author by `userId`

```
intro → fetch_post → fetch_post_author → summary
```

**Required connection:** Add `jsonplaceholder_api` in Admin → Connections:
- Connection Type: `HTTP`
- Host: `https://jsonplaceholder.typicode.com`

---

### 05 — SSHOperator + XCom ETL (`ssh_taskflow_etl`)

Advanced pattern mixing `SSHOperator` with the TaskFlow API to run a remote ETL script and pass its JSON output back via XCom.

- `build_command` (@task): constructs the remote command string using the logical date (`ds`)
- `SSHOperator`: executes `etl.py` on the remote host; stdout is captured as XCom
- `parse_result` (@task): base64-decodes the raw XCom bytes and parses JSON
- `validate_results` (@task): asserts `failed_rows ≤ 100` and extracts the output path
- `trigger_downstream` (@task): notifies downstream systems

```
build_command → run_remote_etl (SSHOperator) → parse_result → validate_results → trigger_downstream
```

**Required connection:** `my_remote_server` (SSH) — configure host, username, and key in Admin → Connections.

Schedule: `@daily` with 2 retries (5-minute delay).

---

## Scripts

### `scripts/etl.py`

Standalone ETL script intended to run on a remote host. Accepts `--date` (YYYY-MM-DD) and `--env` flags. Outputs a JSON result to stdout (logs go to stderr to keep XCom clean):

```json
{
  "rows_processed": 42000,
  "failed_rows": 3,
  "output_path": "s3://my-bucket/output/YYYY-MM-DD/",
  "status": "success"
}
```

### `test_ssh_local.py`

Local smoke-test for the SSH connection. Reads `SSH_USERNAME` and `SSH_PASSWORD` from environment variables and connects to `localhost:2222` to run the ETL script.

```bash
SSH_USERNAME=<user> SSH_PASSWORD=<pass> python test_ssh_local.py
```

---

## Getting Started

### Prerequisites

- Python 3.9 or later
- Docker + Docker Compose (recommended for local setup)

**Airflow 3 core:**
```bash
pip install apache-airflow==3.0.0
```

**Required providers:**
```bash
pip install \
  apache-airflow-providers-standard \
  apache-airflow-providers-http \
  apache-airflow-providers-ssh
```

Or install everything together with constraints (recommended):
```bash
pip install apache-airflow==3.0.0 \
  apache-airflow-providers-standard \
  apache-airflow-providers-http \
  apache-airflow-providers-ssh \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"
```

| Provider | Used by |
|----------|---------|
| `apache-airflow-providers-standard` | BashOperator, PythonOperator, BranchPythonOperator, FileSensor |
| `apache-airflow-providers-http` | HttpHook (DAG 04) |
| `apache-airflow-providers-ssh` | SSHOperator, SSHHook (DAG 05) |

### Running Locally

#### Option A — Astro CLI (recommended)

1. Install the Astro CLI:
   ```bash
   # macOS
   brew install astro
   ```

2. Initialize an Astro project in this directory (first time only):
   ```bash
   astro dev init
   ```

3. Start the local Airflow environment:
   ```bash
   astro dev start
   ```

4. Open the Airflow UI at `http://localhost:8080` (default credentials: `admin` / `admin`).

5. To stop:
   ```bash
   astro dev stop
   ```

Other useful Astro CLI commands:
```bash
astro dev restart   # restart containers after config changes
astro dev ps        # list running containers
astro dev logs      # tail scheduler/webserver logs
```

#### Option B — Docker Compose

1. Start Airflow:
   ```bash
   docker compose up -d
   ```

2. Open the Airflow UI at `http://localhost:8080` (default credentials: `airflow` / `airflow`).

3. Enable and trigger any DAG from the UI.

---

## CI/CD — DAG Linting & Anti-Pattern Checks

Every push or pull request that touches `dags/` triggers the [DAG Anti-Pattern Checker](https://github.com/Shrividya/airflow-dag-anti-pattern-checker) via GitHub Actions (`.github/workflows/dag-lint.yml`).

### What it checks

The checker performs static analysis across 5 categories without needing an Airflow installation:

| Category | Examples |
|----------|---------|
| Top-level code | DB calls, API requests, heavy imports at parse time |
| Retry config | Missing retries, misconfigured retry delays |
| Hardcoding | Credentials, `datetime.now()`, hardcoded file paths |
| Dependencies | Undefined task relationships, `catchup=True` risks |
| Task atomicity | Monolithic callables, unstructured dynamic loops |

### Pipeline behaviour

| Step | Severity | Behaviour |
|------|----------|-----------|
| `Fail on HIGH severity violations` | HIGH | **Blocks the pipeline** — must be fixed before merge |
| `Report MEDIUM and LOW violations` | MEDIUM / LOW | Runs after, informational only — never blocks |

### Running locally before pushing

```bash
pip install airflow-antipattern

# Block on high-severity issues (mirrors CI)
airflow-antipattern check dags/ --severity=high

# See all findings with fix suggestions
airflow-antipattern check dags/ --fix

# Investigate a single DAG
airflow-antipattern check dags/05_sshoperator_xcom_dag.py --why
```

To suppress a known false-positive inline:
```python
conn = psycopg2.connect(os.environ["DB"])  # noqa: TLC001
```

---

### Recommended Learning Order

Work through the DAGs in numbered order — each builds on concepts introduced in the previous one:

```
01 TaskFlow API → 02 Operators → 03 Sensors → 04 Hooks → 05 SSHOperator + XCom
```
