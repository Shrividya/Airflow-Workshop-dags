# airflow-unstructured-rag

A research paper ingestion pipeline using:

- [unstructured.io](https://unstructured.io) for PDF parsing and chunking
- Apache Airflow 3.x for orchestration
- common-ai provider (`@task.llm`) for LLM summarization via Claude Haiku
- HITLBranchOperator for human review before saving output

## Pipeline Flow

```
scan_papers -> parse_and_chunk -> summarize_chunks -> hitl_review -> save_output
                                                                  -> log_rejection
```

---

## Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ (for the download script)
- An Anthropic API key: [console.anthropic.com](https://console.anthropic.com)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/airflow-unstructured-rag.git
cd airflow-unstructured-rag
```

### 2. Configure your environment

Open `.env` and add Model API key.

Never commit `.env` to git. It is in `.gitignore` already.

Drop your own `.pdf` files into `data/paper/`.

### 3. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Wait about 60 seconds, then open http://localhost:8080

### 5. Add the Anthropic connection

In the Airflow UI go to Admin, then Connections, then click the plus button and fill in:

| Field     | Value |
|-----------|-------|
| Conn ID   | `anthropic_default` |
| Conn Type | `pydanticai` |
| Extra     | `{"model": "<model name>"}` |
| Password  | your Model's API key |

### 6. Trigger the DAG

1. Find `pdf_research_pipeline` in the Airflow UI
2. Click Trigger
3. Watch `scan_papers`, `parse_and_chunk`, and `summarize_chunks` complete
4. The pipeline pauses at `hitl_review`
5. Open the XCom tab of `summarize_chunks` to review the summaries
6. Choose Approve or Reject in the HITL task
7. Approved output saves to `output/summaries_YYYYMMDD_HHMMSS.json`

---

## Project Structure

Entire folder structure has not been committed to only commit essential changes.
```
.
├── dags/
│   └── pdf_research.py
├── data/
│   └── paper/
├── output/
├── docker-compose.yml
├── Dockerfile
├── .env
├── .gitignore
└── README.md
```

---

## Customization

| What | Where | How |
|------|-------|-----|
| Change model | Airflow connection `anthropic_default`(in this case) | Update `model` in the Extra JSON field |
| Process more chunks | `summarize_chunks` in `dags/pdf_research.py` | Change `chunks[:20]` limit |
| Use hi-res OCR | `parse_and_chunk` task | Change `strategy="fast"` to `"hi_res"` |
| Add vector store | After `save_output` | Add a new task using chromadb |

---

## Stopping

```bash
docker compose down
docker compose down -v
```
