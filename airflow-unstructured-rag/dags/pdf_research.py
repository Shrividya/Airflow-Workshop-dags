from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.providers.common.ai.operators.llm import LLMOperator

PAPERS_DIR = Path("/opt/airflow/data/paper")
OUTPUT_DIR = Path("/opt/airflow/output")
LLM_CONN_ID = "anthropic_default"
MODEL = "claude-haiku-4-5-20251001"

@dag(
    dag_id="pdf_research_pipeline",
    description="Parse research PDFs with unstructured, summarize with Claude Haiku, review with HITL",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["unstructured", "rag", "hitl", "llm"],
)
def pdf_research_pipeline():
    @task
    def scan_papers() -> list[str]:
        PAPERS_DIR.mkdir(parents=True, exist_ok=True)
        pdfs = [str(p) for p in PAPERS_DIR.glob("*.pdf")]
        if not pdfs:
            raise ValueError(
                f"No PDFs found in {PAPERS_DIR}. "
                "Drop some .pdf files into data// and re-trigger the DAG."
            )
        print(f"Found {len(pdfs)} PDF(s): {pdfs}")
        return pdfs

    @task
    def parse_and_chunk(pdf_paths: list[str]) -> list[dict]:
        """
        Use unstructured to partition each PDF into elements,
        then chunk by title so each chunk is a coherent section.
        Returns a list of chunk dicts ready for the LLM.
        """
        from unstructured.chunking.title import chunk_by_title
        from unstructured.partition.pdf import partition_pdf

        all_chunks = []
        for pdf_path in pdf_paths:
            print(f"Parsing: {pdf_path}")
            elements = partition_pdf(
                filename=pdf_path,
                strategy="fast",
                infer_table_structure=True,
            )
            chunks = chunk_by_title(
                elements,
                max_characters=1500,
                new_after_n_chars=1000,
            )
            filename = Path(pdf_path).name
            for i, chunk in enumerate(chunks):
                all_chunks.append({
                    "source_file": filename,
                    "chunk_index": i,
                    "category": chunk.category,
                    "text": chunk.text,
                    "page_number": getattr(chunk.metadata, "page_number", None),
                })
            print(f"  → {len(chunks)} chunks from {filename}")

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
        for c in chunks[:20]:  # cap at 20 chunks
            formatted.append(
                f"[File: {c['source_file']} | Page: {c['page_number']} | Chunk {c['chunk_index']}]\n"
                f"{c['text'][:800]}"
            )
        combined = "\n\n---\n\n".join(formatted)
        return (
            f"Summarize each of the following {len(formatted)} chunks from research paper. "
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

    # HITL branches to either save_output or log_rejection
    hitl_review.set_upstream(llm_out)
    save_output(llm_out, chunks) >> hitl_review  # type: ignore[operator]

    # targets
    approved = save_output.override(task_id="save_output")(llm_out, chunks)
    rejected = log_rejection.override(task_id="log_rejection")()

    hitl_review >> [approved, rejected]


pdf_research_pipeline()