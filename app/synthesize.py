"""Citation-backed literature synthesis with self-feedback refinement.

Inspired by OpenScholar (Asai et al., Nature 2026):
- Generates citation-backed synthesis from search results
- Self-feedback loop: draft -> critique -> retrieve more -> refine
- Uses Claude API for generation
"""

import os
import json
import sqlite3
import logging
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


def _get_chunks_for_publication(pub_id: str, limit: int = 3) -> list:
    """Get the most relevant text chunks for a publication."""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT chunk_text FROM chunks WHERE publication_id = ? ORDER BY chunk_index LIMIT ?",
            (pub_id, limit),
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def _build_context(publications: list, query: str) -> str:
    """Build a rich context string from publications with chunks."""
    context_parts = []

    for i, pub in enumerate(publications[:10], 1):
        pub_id = pub.get("id", "")
        title = pub.get("title", "Untitled")
        year = pub.get("year", "")
        journal = pub.get("journal", "")
        abstract = pub.get("abstract", "") or pub.get("abstract_short", "")
        doi = pub.get("doi", "")
        authors = ", ".join(
            a.get("display_name", "") for a in pub.get("authors_list", [])[:5]
        )
        if len(pub.get("authors_list", [])) > 5:
            authors += " et al."

        # Get full-text chunks for richer context
        chunks = _get_chunks_for_publication(pub_id, limit=2)
        chunk_text = ""
        if chunks:
            chunk_text = "\nKey excerpts:\n" + "\n".join(
                f"  > {c[:400]}" for c in chunks
            )

        context_parts.append(
            f"[{i}] {title}\n"
            f"    Authors: {authors}\n"
            f"    Year: {year} | Journal: {journal}\n"
            f"    DOI: {doi}\n"
            f"    Abstract: {abstract[:500]}"
            f"{chunk_text}"
        )

    return "\n\n".join(context_parts)


def _call_claude(messages: list, system: str = "", max_tokens: int = 2000) -> str:
    """Call Claude API. Uses requests to avoid heavy SDK dependency."""
    import requests

    if not ANTHROPIC_API_KEY:
        return ""

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "content-type": "application/json",
        "anthropic-version": "2023-06-01",
    }

    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "messages": messages,
    }
    if system:
        body["system"] = system

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=body,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("content", [{}])[0].get("text", "")
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return ""


def synthesize(query: str, publications: list, refine: bool = True) -> dict:
    """Generate a citation-backed synthesis of search results.

    Uses a 3-step OpenScholar-inspired pipeline:
    1. Initial synthesis with inline citations
    2. Self-feedback: critique the draft
    3. Refinement: improve based on critique

    Returns dict with:
        - synthesis: final text with [1], [2] etc. citation markers
        - references: list of {number, title, authors, year, doi}
        - feedback: the self-critique (if refine=True)
        - draft: the initial draft (if refine=True)
    """
    if not ANTHROPIC_API_KEY:
        return {
            "error": "No API key configured. Set ANTHROPIC_API_KEY environment variable.",
            "synthesis": "",
            "references": [],
        }

    if not publications:
        return {"synthesis": "No publications to synthesize.", "references": []}

    context = _build_context(publications, query)

    # Build reference list
    references = []
    for i, pub in enumerate(publications[:10], 1):
        authors = ", ".join(
            a.get("display_name", "") for a in pub.get("authors_list", [])[:3]
        )
        if len(pub.get("authors_list", [])) > 3:
            authors += " et al."
        references.append({
            "number": i,
            "title": pub.get("title", ""),
            "authors": authors,
            "year": pub.get("year", ""),
            "journal": pub.get("journal", ""),
            "doi": pub.get("doi", ""),
        })

    system_prompt = (
        "You are a scientific literature synthesis assistant for IGB "
        "(Leibniz Institute of Freshwater Ecology and Inland Fisheries). "
        "You write clear, accurate summaries of research findings. "
        "Always cite sources using [N] notation matching the provided references. "
        "Every factual claim must have at least one citation. "
        "Be concise but comprehensive. Focus on key findings, methods, and trends."
    )

    # Step 1: Initial synthesis
    draft_prompt = (
        f"Based on the following IGB publications, write a synthesis that addresses "
        f"this research query:\n\n"
        f"QUERY: {query}\n\n"
        f"PUBLICATIONS:\n{context}\n\n"
        f"Write a 2-4 paragraph synthesis with inline citations [1], [2], etc. "
        f"Cover: (1) the main findings relevant to the query, "
        f"(2) methodological approaches used, "
        f"(3) any consensus or disagreements in the literature, "
        f"(4) gaps or opportunities for future research. "
        f"Every claim must cite at least one source."
    )

    draft = _call_claude(
        [{"role": "user", "content": draft_prompt}],
        system=system_prompt,
        max_tokens=1500,
    )

    if not draft:
        return {
            "error": "Failed to generate synthesis. Check API key and try again.",
            "synthesis": "",
            "references": references,
        }

    if not refine:
        return {
            "synthesis": draft,
            "references": references,
            "draft": draft,
        }

    # Step 2: Self-feedback
    feedback_prompt = (
        f"You wrote the following literature synthesis:\n\n"
        f"---\n{draft}\n---\n\n"
        f"Available sources:\n{context}\n\n"
        f"Critique this synthesis on:\n"
        f"1. CITATION ACCURACY: Are all claims properly cited? Any unsupported statements?\n"
        f"2. COVERAGE: Are important findings from the sources missing?\n"
        f"3. BALANCE: Does it fairly represent different perspectives?\n"
        f"4. CLARITY: Is the writing clear and well-organized?\n"
        f"5. GAPS: What important aspects of the query are not addressed?\n\n"
        f"Be specific about what needs improvement."
    )

    feedback = _call_claude(
        [{"role": "user", "content": feedback_prompt}],
        system="You are a critical reviewer of scientific literature syntheses. Be specific and constructive.",
        max_tokens=800,
    )

    if not feedback:
        return {
            "synthesis": draft,
            "references": references,
            "draft": draft,
        }

    # Step 3: Refinement based on feedback
    refine_prompt = (
        f"Here is a literature synthesis and its critique:\n\n"
        f"ORIGINAL QUERY: {query}\n\n"
        f"DRAFT:\n{draft}\n\n"
        f"CRITIQUE:\n{feedback}\n\n"
        f"AVAILABLE SOURCES:\n{context}\n\n"
        f"Rewrite the synthesis addressing the critique. "
        f"Ensure every factual claim has an inline citation [N]. "
        f"Maintain 2-4 paragraphs. Be concise but thorough."
    )

    refined = _call_claude(
        [{"role": "user", "content": refine_prompt}],
        system=system_prompt,
        max_tokens=1500,
    )

    return {
        "synthesis": refined or draft,
        "references": references,
        "draft": draft,
        "feedback": feedback,
    }


def is_available() -> bool:
    """Check if synthesis is available (API key configured)."""
    return bool(ANTHROPIC_API_KEY)
