# Airflow Workshop DAGs

A hands-on repository for learning **Apache Airflow 3** — from core concepts to AI-powered pipelines. Two self-contained projects, each with its own Docker Compose setup and README.

---

## Projects

### [`workshop_dags/`](./workshop_dags/README.md) — Core Concepts Workshop

Five progressive DAGs covering the fundamentals of Airflow 3 authoring, designed as a workshop curriculum:

| # | DAG | Concepts |
|---|-----|----------|
| 01 | `demo_01_taskflow_api` | `@task` / `@dag` decorators, ETL pipeline |
| 02 | `demo_02_operators` | BashOperator, PythonOperator, BranchPythonOperator, XCom, TriggerRule |
| 03 | `demo_03_sensors` | FileSensor, poke mode, Airflow connections |
| 04 | `demo_04_hooks` | HttpHook, named connections, XCom chaining |
| 05 | `ssh_taskflow_etl` | SSHOperator, remote XCom capture, TaskFlow + classic operator mix |

**Recommended learning order:** `01 → 02 → 03 → 04 → 05`

---

### [`airflow-unstructured-rag/`](./airflow-unstructured-rag/README.md) — AI Research Paper Pipeline

An end-to-end PDF ingestion pipeline using Airflow 3's `@task.llm` decorator and `HITLOperator`:

```
scan_papers → parse_and_chunk → summarize_chunks → [HITL Review] → save_output
                                  (Claude Haiku)     (you approve)
```

Two DAG variants are included — one using `unstructured` (supports OCR, tables) and one using `pdfplumber` (lighter install, clean text PDFs). Summaries are gated behind a human-in-the-loop review step before being written to disk.

**Requires:** Docker + Docker Compose, Anthropic API key.

---

## CI/CD

Every push or pull request touching `dags/` triggers the [DAG Anti-Pattern Checker](https://github.com/Shrividya/airflow-dag-anti-pattern-checker) via GitHub Actions:

- **HIGH severity violations** — block the pipeline (must fix before merge)
- **MEDIUM / LOW violations** — reported as informational, never block

Run the same check locally:
```bash
pip install airflow-antipattern
airflow-antipattern check dags/ --severity=high
```

---

## Quick Start

Each project has its own setup guide. Follow the README in the directory you want to explore:

- [workshop_dags/README.md](./workshop_dags/README.md)
- [airflow-unstructured-rag/README.md](./airflow-unstructured-rag/README.md)
