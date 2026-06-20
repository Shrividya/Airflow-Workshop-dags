# airflow-unstructured-rag

A research paper ingestion pipeline built on **Apache Airflow 3.x** that parses PDFs, summarizes them with Claude Haiku via the `@task.llm` decorator, and gates the output behind a human-in-the-loop review step.

Two DAGs are included — pick the one that fits your environment:

| DAG | File | PDF parser | Best for |
|-----|------|-----------|----------|
| `pdf_research_pipeline` | `pdf_research.py` | `unstructured` | Complex layouts, tables, OCR |
| `pdf_research_pipeline_pdfplumber` | `pdf_research_pdfplumber.py` | `pdfplumber` | Clean text-based PDFs, lighter install |

## Pipeline Flow

```
scan_papers → parse_and_chunk → summarize_chunks → [HITL Review] → save_output
                                  (Claude Haiku)     (you decide)       OR
                                                                    log_rejection
```

---

## Prerequisites

- Docker & Docker Compose
- An Anthropic API key → [console.anthropic.com](https://console.anthropic.com)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/airflow-unstructured-rag.git
cd airflow-unstructured-rag
```

### 2. Configure environment

Create a `.env` file in the project root with the following variables:

```bash
ANTHROPIC_API_KEY=sk-ant-your-key-here
FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW_UID=50000
```

> Never commit `.env` to git — it is already in `.gitignore`.

### 3. Add PDF files

Drop your `.pdf` files into `data/paper/`.

### 4. Build and start Airflow

```bash
# First time only — build the image and initialize the DB
docker compose up airflow-init

# Start all services
docker compose up -d
```

Wait ~60 seconds, then open: **http://localhost:8080**

Login: `airflow` / `airflow`

### 5. Create the Anthropic connection

In the Airflow UI go to **Admin → Connections → +** and add:

| Field | Value |
|-------|-------|
| Connection Id | `anthropic_default` |
| Connection Type | `HTTP` (or `Generic`) |
| Password | your Anthropic API key |

### 6. Trigger a DAG

1. Find `pdf_research_pipeline` (or `pdf_research_pipeline_pdfplumber`) in the DAG list
2. Click **Trigger**
3. Watch: `scan_papers` → `parse_and_chunk` → `summarize_chunks`
4. The pipeline **pauses** at `hitl_review` and shows a review prompt
5. In the HITL task panel, open the **XCom** tab of `summarize_chunks` to read the summaries
6. Choose **Approve** → output saved to `output/summaries_YYYYMMDD_HHMMSS.json`
   or **Reject** → pipeline stops, nothing is saved

---

## Project Structure

```
.
├── dags/
│   ├── pdf_research.py                  # DAG using unstructured for PDF parsing
│   └── pdf_research_pdfplumber.py       # DAG using pdfplumber for PDF parsing
├── data/
│   └── paper/                           # Drop your PDFs here
├── output/                              # Approved summaries saved here (JSON)
├── config/
│   └── airflow.cfg                      # Airflow configuration
├── Dockerfile                           # Extends apache/airflow:3.2.2
├── docker-compose.yaml
├── requirements.txt
└── .env                                 # Your secrets — NEVER commit
```

---

## DAG Details

### `pdf_research_pipeline` (unstructured-based)

- Uses `unstructured[pdf]` to partition PDFs into semantic elements
- Chunks by title (`chunk_by_title`) with 1500-char max / 1000-char soft limit
- Supports tables and OCR (`strategy="fast"` by default; switch to `"hi_res"` for scanned docs)

### `pdf_research_pipeline_pdfplumber` (pdfplumber-based)

- Uses `pdfplumber` to extract text page by page
- Splits into paragraph-boundary chunks (same size limits)
- No system-level OCR dependencies needed — easier to install

Both DAGs share the same chunk schema and produce identical output JSON.

---

## Dependencies

**Python packages** (`requirements.txt`):

```
apache-airflow-providers-common-ai
apache-airflow-providers-standard
unstructured[pdf]==0.17.2
pydantic-ai-slim[anthropic]
sentence-transformers==3.3.1
pdfplumber==0.11.4
```

**System packages** (installed in `Dockerfile`):

```
poppler-utils, tesseract-ocr, tesseract-ocr-eng, libmagic1, libgl1, libglib2.0-0
```

**Additional pip** (in `Dockerfile`):

```
apache-airflow-providers-common-ai
chromadb>=0.5.0
```

---

## Customization

| What | Where | How |
|------|-------|-----|
| Change model | `dags/pdf_research.py` | Update `MODEL` constant (default: `claude-haiku-4-5-20251001`) |
| Process more chunks | `summarize_chunks` task | Change `chunks[:20]` cap |
| Use hi-res OCR | `parse_and_chunk` in unstructured DAG | Change `strategy="fast"` → `"hi_res"` |
| Add vector store | After `save_output` | Add a task using `chromadb` (already installed) |

---

## Stopping

```bash
docker compose down          # stop containers
docker compose down -v       # stop + delete volumes (full reset)
```
