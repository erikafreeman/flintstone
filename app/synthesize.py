"""Citation-backed literature synthesis with self-feedback refinement.

Inspired by OpenScholar (Asai et al., Nature 2026):
- Generates citation-backed synthesis from search results
- Self-feedback loop: draft -> critique -> retrieve more -> refine
- Semantic Scholar API for citation verification & supplementary retrieval
- Works with or without Claude API key (local extractive fallback)
"""

import os
import re
import json
import sqlite3
import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
S2_API_KEY = os.environ.get("S2_API_KEY", "")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


# ---------------------------------------------------------------------------
# Semantic Scholar API integration (OpenScholar-style)
# ---------------------------------------------------------------------------

def _s2_headers():
    h = {"User-Agent": "Feuerstein/1.0 (IGB Publication Intelligence)"}
    if S2_API_KEY:
        h["x-api-key"] = S2_API_KEY
    return h


def verify_citations(references: list) -> list:
    """Verify that cited papers actually exist via Semantic Scholar API.

    Returns references list with added 's2_verified' and 's2_url' fields.
    OpenScholar found 78-90% of LLM citations are hallucinated — we check.
    """
    for ref in references:
        ref["s2_verified"] = False
        ref["s2_url"] = ""

        title = ref.get("title", "")
        if not title:
            continue

        try:
            resp = requests.get(
                "https://api.semanticscholar.org/graph/v1/paper/search/match",
                params={"query": title[:300], "fields": "title,url,citationCount,year"},
                headers=_s2_headers(),
                timeout=10,
            )
            time.sleep(0.3)  # rate limit

            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    match = data[0]
                    # Fuzzy title match — first 50 chars lowercase
                    s2_title = (match.get("title") or "").lower()[:50]
                    our_title = title.lower()[:50]
                    if s2_title and (s2_title in our_title or our_title in s2_title):
                        ref["s2_verified"] = True
                        ref["s2_url"] = match.get("url", "")
                        ref["s2_citations"] = match.get("citationCount", 0)
        except Exception as e:
            logger.debug(f"S2 verify failed for '{title[:40]}': {e}")

    return references


def search_supplementary(query: str, exclude_titles: set, limit: int = 5) -> list:
    """Search Semantic Scholar for supplementary papers not in our database.

    OpenScholar uses this during the self-feedback loop to find missing evidence.
    """
    results = []
    try:
        resp = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params={
                "query": query[:200],
                "limit": limit,
                "fields": "title,abstract,year,url,authors,citationCount,journal",
                "sort": "citationCount:desc",
            },
            headers=_s2_headers(),
            timeout=15,
        )
        time.sleep(0.5)

        if resp.status_code == 200:
            for paper in resp.json().get("data", []):
                title = paper.get("title", "")
                if title.lower() in {t.lower() for t in exclude_titles}:
                    continue
                authors = ", ".join(
                    a.get("name", "") for a in (paper.get("authors") or [])[:3]
                )
                if len(paper.get("authors") or []) > 3:
                    authors += " et al."
                results.append({
                    "title": title,
                    "abstract": (paper.get("abstract") or "")[:300],
                    "year": paper.get("year"),
                    "url": paper.get("url", ""),
                    "authors": authors,
                    "citations": paper.get("citationCount", 0),
                    "journal": (paper.get("journal") or {}).get("name", ""),
                    "source": "Semantic Scholar",
                })
    except Exception as e:
        logger.debug(f"S2 search failed: {e}")

    return results


# ---------------------------------------------------------------------------
# Full-text chunk retrieval
# ---------------------------------------------------------------------------

def _get_chunks_for_publication(pub_id: str, limit: int = 3) -> list:
    """Get text chunks for a publication."""
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


# ---------------------------------------------------------------------------
# Context building
# ---------------------------------------------------------------------------

def _build_context(publications: list, query: str, supplementary: list = None) -> str:
    """Build rich context from publications + optional S2 supplementary papers."""
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

    # Add supplementary S2 papers as additional references
    if supplementary:
        offset = len(publications[:10])
        for j, sp in enumerate(supplementary, 1):
            idx = offset + j
            context_parts.append(
                f"[{idx}] {sp['title']} [Supplementary — from Semantic Scholar]\n"
                f"    Authors: {sp['authors']}\n"
                f"    Year: {sp.get('year', '?')} | Journal: {sp.get('journal', '')}\n"
                f"    Abstract: {sp.get('abstract', '')[:400]}"
            )

    return "\n\n".join(context_parts)


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

def _call_claude(messages: list, system: str = "", max_tokens: int = 2000) -> str:
    """Call Claude API via direct HTTP."""
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


# ---------------------------------------------------------------------------
# Local extractive synthesis (no API key needed)
# ---------------------------------------------------------------------------

def _extractive_synthesis(query: str, publications: list) -> str:
    """Build a structured extractive synthesis without an LLM.

    Uses key sentences from abstracts and chunks, organized thematically.
    """
    if not publications:
        return ""

    lines = []
    lines.append(
        f"**Literature overview** based on {len(publications[:10])} IGB publications "
        f"matching your query.\n"
    )

    # Group findings
    for i, pub in enumerate(publications[:10], 1):
        title = pub.get("title", "Untitled")
        year = pub.get("year", "")
        abstract = pub.get("abstract", "") or ""

        # Extract first substantive sentence from abstract
        sentences = re.split(r'(?<=[.!?])\s+', abstract)
        key_sentence = ""
        for s in sentences:
            # Skip generic opening sentences
            if len(s) > 60 and not s.lower().startswith(("here we", "in this", "this study", "this paper", "we present", "abstract")):
                key_sentence = s.strip()
                break
        if not key_sentence and sentences:
            key_sentence = sentences[0].strip()

        if key_sentence:
            # Truncate very long sentences
            if len(key_sentence) > 250:
                key_sentence = key_sentence[:247] + "..."
            lines.append(f"- {key_sentence} [{i}]")

    # Add methods overview
    chunks_with_methods = []
    for i, pub in enumerate(publications[:5], 1):
        pub_id = pub.get("id", "")
        chunks = _get_chunks_for_publication(pub_id, limit=3)
        for c in chunks:
            cl = c.lower()
            if any(kw in cl for kw in ("method", "approach", "experiment", "measured", "sampled", "monitored")):
                # Extract first sentence of method-related chunk
                first_sent = re.split(r'(?<=[.!?])\s+', c)[0]
                if 30 < len(first_sent) < 200:
                    chunks_with_methods.append((first_sent, i))
                    break

    if chunks_with_methods:
        lines.append("\n**Key methods:**")
        for sent, ref_num in chunks_with_methods[:3]:
            lines.append(f"- {sent} [{ref_num}]")

    lines.append(
        "\n*This is an extractive summary built from publication abstracts and full-text chunks. "
        "For AI-generated synthesis with self-feedback refinement, configure an Anthropic API key.*"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main synthesis function
# ---------------------------------------------------------------------------

def synthesize(query: str, publications: list, refine: bool = True) -> dict:
    """Generate a citation-backed synthesis of search results.

    Pipeline (OpenScholar-inspired):
    1. Build context from publications + full-text chunks
    2. Search Semantic Scholar for supplementary papers
    3. Generate initial synthesis with inline citations
    4. Self-feedback: critique the draft
    5. Refinement: improve based on critique + supplementary evidence
    6. Verify citations via Semantic Scholar API

    Falls back to extractive synthesis if no API key is set.
    """
    if not publications:
        return {"synthesis": "No publications to synthesize.", "references": []}

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

    # Search Semantic Scholar for supplementary papers
    existing_titles = {pub.get("title", "") for pub in publications[:10]}
    supplementary = search_supplementary(query, existing_titles, limit=3)

    # Add supplementary to references
    for j, sp in enumerate(supplementary):
        references.append({
            "number": len(publications[:10]) + j + 1,
            "title": sp["title"],
            "authors": sp["authors"],
            "year": sp.get("year", ""),
            "journal": sp.get("journal", ""),
            "doi": "",
            "s2_url": sp.get("url", ""),
            "source": "Semantic Scholar",
        })

    # Fallback: extractive synthesis (no API key needed)
    if not ANTHROPIC_API_KEY:
        synthesis_text = _extractive_synthesis(query, publications)

        # Still verify citations via S2
        references = verify_citations(references)

        return {
            "synthesis": synthesis_text,
            "references": references,
            "supplementary": supplementary,
            "mode": "extractive",
        }

    # Full LLM pipeline
    context = _build_context(publications, query, supplementary)

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
        f"Based on the following publications, write a synthesis that addresses "
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
        # Fallback to extractive if API call fails
        synthesis_text = _extractive_synthesis(query, publications)
        references = verify_citations(references)
        return {
            "synthesis": synthesis_text,
            "references": references,
            "supplementary": supplementary,
            "mode": "extractive",
            "error": "Claude API call failed — showing extractive summary instead.",
        }

    if not refine:
        references = verify_citations(references)
        return {
            "synthesis": draft,
            "references": references,
            "draft": draft,
            "supplementary": supplementary,
            "mode": "generative",
        }

    # Step 2: Self-feedback (OpenScholar-style)
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
        references = verify_citations(references)
        return {
            "synthesis": draft,
            "references": references,
            "draft": draft,
            "supplementary": supplementary,
            "mode": "generative",
        }

    # Step 3: Refinement with feedback + supplementary evidence
    refine_prompt = (
        f"Here is a literature synthesis and its critique:\n\n"
        f"ORIGINAL QUERY: {query}\n\n"
        f"DRAFT:\n{draft}\n\n"
        f"CRITIQUE:\n{feedback}\n\n"
        f"AVAILABLE SOURCES:\n{context}\n\n"
        f"Rewrite the synthesis addressing the critique. "
        f"Ensure every factual claim has an inline citation [N]. "
        f"Maintain 2-4 paragraphs. Be concise but thorough. "
        f"You may use the supplementary sources from Semantic Scholar if they strengthen the synthesis."
    )

    refined = _call_claude(
        [{"role": "user", "content": refine_prompt}],
        system=system_prompt,
        max_tokens=1500,
    )

    # Step 4: Verify citations via Semantic Scholar
    references = verify_citations(references)

    return {
        "synthesis": refined or draft,
        "references": references,
        "draft": draft,
        "feedback": feedback,
        "supplementary": supplementary,
        "mode": "generative",
    }


def is_available() -> bool:
    """Synthesis is always available — extractive fallback if no API key."""
    return True
