from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.standard.operators.hitl import HITLBranchOperator

PAPERS_DIR = Path("/opt/airflow/data/paper")
OUTPUT_DIR = Path("/opt/airflow/output")
LLM_CONN_ID = "anthropic_default"

MAX_CHUNK_CHARS = 1500
SOFT_LIMIT_CHARS = 1000
MAX_CHUNKS_FOR_LLM = 20


@dag(
    dag_id="pdf_research_pipeline_pdfplumber",
    description="Parse research PDFs with pdfplumber, summarize with Claude Haiku, review with HITL",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["pdfplumber", "rag", "hitl", "llm"],
)
def pdf_research_pipeline_pdfplumber():
    @task
    def scan_papers() -> list[str]:
        PAPERS_DIR.mkdir(parents=True, exist_ok=True)
        pdfs = [str(p) for p in PAPERS_DIR.glob("*.pdf")]
        if not pdfs:
            raise ValueError(
                f"No PDFs found in {PAPERS_DIR}. "
                "Drop some .pdf files into data/paper/ and re-trigger the DAG."
            )
        print(f"Found {len(pdfs)} PDF(s): {pdfs}")
        return pdfs

    @task
    def parse_and_chunk(pdf_paths: list[str]) -> list[dict]:
        """
        Use pdfplumber to extract text page by page, then split into
        chunks at paragraph boundaries respecting a max character limit.
        Produces the same chunk dict schema as the unstructured-based DAG.
        """
        import pdfplumber

        def _chunk_page_text(text: str) -> list[str]:
            paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
            chunks: list[str] = []
            current_parts: list[str] = []
            current_len = 0

            for para in paragraphs:
                para_len = len(para)
                if current_len + para_len > MAX_CHUNK_CHARS and current_parts:
                    chunks.append("\n\n".join(current_parts))
                    current_parts = [para]
                    current_len = para_len
                else:
                    current_parts.append(para)
                    current_len += para_len
                    if current_len >= SOFT_LIMIT_CHARS:
                        chunks.append("\n\n".join(current_parts))
                        current_parts = []
                        current_len = 0

            if current_parts:
                chunks.append("\n\n".join(current_parts))
            return chunks

        all_chunks: list[dict] = []
        for pdf_path in pdf_paths:
            print(f"Parsing: {pdf_path}")
            filename = Path(pdf_path).name
            chunk_index = 0

            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    page_text = page.extract_text() or ""
                    if not page_text.strip():
                        continue
                    for chunk_text in _chunk_page_text(page_text):
                        all_chunks.append(
                            {
                                "source_file": filename,
                                "chunk_index": chunk_index,
                                "category": "NarrativeText",
                                "text": chunk_text,
                                "page_number": page_num,
                            }
                        )
                        chunk_index += 1

            print(f"  → {chunk_index} chunks from {filename}")

        print(f"Total chunks across all PDFs: {len(all_chunks)}")
        return all_chunks

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a research assistant. Given a chunk of text from a research paper, "
            "produce a concise 2-3 sentence summary that captures: (1) the key claim or finding, "
            "(2) the method or evidence, and (3) why it matters. "
            "Be precise and avoid filler phrases. "
            "Respond with a JSON object: "
            '{"summary": "...", "key_terms": ["term1", "term2"], "importance": "high|medium|low"}'
        ),
        output_type=str,
    )
    def summarize_chunks(chunks: list[dict]) -> str:
        """Ask the LLM to summarize all parsed chunks."""
        formatted = []
        for c in chunks[:MAX_CHUNKS_FOR_LLM]:
            formatted.append(
                f"[File: {c['source_file']} | Page: {c['page_number']} | Chunk {c['chunk_index']}]\n"
                f"{c['text'][:800]}"
            )
        combined = "\n\n---\n\n".join(formatted)
        return (
            f"Summarize each of the following {len(formatted)} chunks from a research paper. "
            f"Return a JSON array where each element has: source_file, chunk_index, summary, key_terms, importance.\n\n"
            f"{combined}"
        )

    hitl_review = HITLBranchOperator(
        task_id="hitl_review",
        subject="Research PDF Summaries — Ready for Review",
        body=(
            "Claude Haiku has summarized your research PDFs.\n\n"
            "**Check the XCom output** of `summarize_chunks` in the Airflow UI to review the summaries.\n\n"
            "- **Approve** → summaries are saved to `/output/` as structured JSON\n"
            "- **Reject** → pipeline stops, nothing is saved (re-trigger after adjusting params)"
        ),
        options=["Approve", "Reject"],
        options_mapping={
            "Approve": "save_output",
            "Reject": "log_rejection",
        },
    )

    @task
    def save_output(llm_response: str, chunks: list[dict]):
        """Parse LLM JSON response and save to output directory."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        clean = llm_response.strip()
        if clean.startswith("```"):
            clean = "\n".join(clean.split("\n")[1:])
        if clean.endswith("```"):
            clean = "\n".join(clean.split("\n")[:-1])

        try:
            summaries = json.loads(clean)
        except json.JSONDecodeError:
            summaries = [{"raw_response": clean}]

        output = {
            "pipeline_run": datetime.now(timezone.utc).isoformat(),
            "total_chunks_parsed": len(chunks),
            "summaries": summaries,
        }

        out_path = OUTPUT_DIR / f"summaries_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        out_path.write_text(json.dumps(output, indent=2))
        print(f"Saved {len(summaries)} summaries to {out_path}")
        return str(out_path)

    @task
    def log_rejection():
        print("Summaries were rejected in HITL review. Pipeline stopped.")
        print("Re-trigger the DAG after reviewing your PDFs or adjusting chunk settings.")

    pdfs = scan_papers()
    chunks = parse_and_chunk(pdfs)
    llm_out = summarize_chunks(chunks)

    hitl_review.set_upstream(llm_out)
    save_output(llm_out, chunks) >> hitl_review  # type: ignore[operator]

    approved = save_output.override(task_id="save_output")(llm_out, chunks)
    rejected = log_rejection.override(task_id="log_rejection")()

    hitl_review >> [approved, rejected]


pdf_research_pipeline_pdfplumber()
