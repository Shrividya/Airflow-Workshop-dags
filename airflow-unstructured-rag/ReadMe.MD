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

```bash
cp .env.example .env
```

Open `.env` and replace `sk-ant-your-key-here` with your real Anthropic API key.

Never commit `.env` to git. It is in `.gitignore` already.

### 3. Download sample papers (optional)

```bash
python scripts/download_sample_papers.py
```

Or drop your own `.pdf` files into `data/paper/`.

### 4. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Wait about 60 seconds, then open http://localhost:8080

Login: `airflow` / `airflow`

### 5. Add the Anthropic connection

In the Airflow UI go to Admin, then Connections, then click the plus button and fill in:

| Field     | Value |
|-----------|-------|
| Conn ID   | `anthropic_default` |
| Conn Type | `pydanticai` |
| Extra     | `{"model": "anthropic:claude-haiku-4-5-20251001"}` |
| Password  | your Anthropic API key |

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

```
.
├── dags/
│   └── pdf_research.py
├── data/
│   └── paper/
├── output/
├── scripts/
│   └── download_sample_papers.py
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── .env
├── .gitignore
└── README.md
```

---

## Customization

| What | Where | How |
|------|-------|-----|
| Change model | Airflow connection `anthropic_default` | Update `model` in the Extra JSON field |
| Process more chunks | `summarize_chunks` in `dags/pdf_research.py` | Change `chunks[:20]` limit |
| Use hi-res OCR | `parse_and_chunk` task | Change `strategy="fast"` to `"hi_res"` |
| Add vector store | After `save_output` | Add a new task using chromadb |

---

## Stopping

```bash
docker compose down
docker compose down -v
```
