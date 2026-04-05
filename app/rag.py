"""RAG Research Assistant — answer questions using IGB publication chunks."""

import os
import re
import sqlite3
import logging
import numpy as np
from typing import Optional

import requests

from . import search

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


def _call_claude(messages: list, system: str = "", max_tokens: int = 2000) -> str:
    """Call Claude API."""
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


def retrieve_chunks(query: str, top_k: int = 15) -> list:
    """Retrieve the most relevant text chunks for a query.

    Returns list of dicts with chunk_text, publication_id, title, year, doi, similarity.
    """
    search.load()

    if search._chunk_embeddings is None or len(search._chunk_embeddings) == 0:
        return []

    # Encode query
    query_emb = search._encode([query])

    # Search chunks
    similarities = np.dot(search._chunk_embeddings, query_emb.T).flatten()
    top_indices = np.argsort(similarities)[::-1][:top_k * 3]  # get extra for dedup

    # Get publication metadata
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    results = []
    seen_chunks = set()  # avoid near-duplicate chunks
    pubs_seen = {}  # limit chunks per publication

    for idx in top_indices:
        if len(results) >= top_k:
            break

        chunk_db_id = int(search._chunk_ids[idx])
        pub_id = search._chunk_pub_map.get(chunk_db_id)
        chunk_text = search._chunk_texts.get(chunk_db_id, "")

        if not pub_id or not chunk_text:
            continue

        # Limit to 3 chunks per publication
        pubs_seen[pub_id] = pubs_seen.get(pub_id, 0) + 1
        if pubs_seen[pub_id] > 3:
            continue

        # Simple dedup: skip if first 100 chars match something we've seen
        chunk_key = chunk_text[:100].lower().strip()
        if chunk_key in seen_chunks:
            continue
        seen_chunks.add(chunk_key)

        # Get full chunk text from DB (search._chunk_texts is truncated to 300 chars)
        row = conn.execute(
            "SELECT chunk_text FROM chunks WHERE id = ?", (chunk_db_id,)
        ).fetchone()
        full_text = row[0] if row else chunk_text

        # Get publication metadata
        meta = search._pub_meta.get(pub_id, {})

        results.append({
            "chunk_text": full_text,
            "publication_id": pub_id,
            "title": meta.get("title", "Unknown"),
            "year": meta.get("year"),
            "similarity": float(similarities[idx]),
        })

    # Also search abstracts for broader coverage
    abstract_results = search._semantic_search(query_emb, 10)
    for i, (idx, sim) in enumerate(abstract_results[:5]):
        pid = search._pub_ids[idx]
        meta = search._pub_meta.get(pid, {})
        if pid not in pubs_seen and meta.get("abstract"):
            results.append({
                "chunk_text": meta["abstract"],
                "publication_id": pid,
                "title": meta.get("title", "Unknown"),
                "year": meta.get("year"),
                "similarity": float(sim),
                "is_abstract": True,
            })

    conn.close()
    return results


def ask(question: str) -> dict:
    """Answer a research question using RAG over IGB publications.

    Returns dict with answer, sources, and mode (generative/extractive).
    """
    # Retrieve relevant chunks
    chunks = retrieve_chunks(question, top_k=15)

    if not chunks:
        return {
            "answer": "I couldn't find any relevant information in the IGB publication database for this question. Try rephrasing or broadening your question.",
            "sources": [],
            "mode": "none",
        }

    # Build context with source attribution
    context_parts = []
    sources = []
    seen_pubs = set()

    for i, chunk in enumerate(chunks):
        pub_id = chunk["publication_id"]
        title = chunk["title"]
        year = chunk.get("year", "")

        # Track unique source publications
        if pub_id not in seen_pubs:
            seen_pubs.add(pub_id)
            source_num = len(sources) + 1
            sources.append({
                "number": source_num,
                "publication_id": pub_id,
                "title": title,
                "year": year,
                "similarity": chunk["similarity"],
            })

        # Find which source number this chunk belongs to
        src_num = next(s["number"] for s in sources if s["publication_id"] == pub_id)

        text = chunk["chunk_text"]
        if len(text) > 600:
            text = text[:600] + "..."

        context_parts.append(
            f"[Source {src_num}: {title} ({year})]\n{text}"
        )

    context = "\n\n---\n\n".join(context_parts)

    # Try generative answer with Claude
    if ANTHROPIC_API_KEY:
        system = (
            "You are Feuerstein, IGB's research assistant. You answer questions about freshwater ecology "
            "and IGB's research using ONLY the provided source excerpts. "
            "Rules:\n"
            "1. Every factual claim must cite its source as [Source N]\n"
            "2. If the sources don't contain enough information, say so honestly\n"
            "3. Be concise but thorough — aim for 2-4 paragraphs\n"
            "4. Highlight any contradictions or debates in the literature\n"
            "5. End with a brief note on what gaps remain based on available sources\n"
            "6. Use clear, accessible scientific language"
        )

        prompt = (
            f"Question: {question}\n\n"
            f"Here are relevant excerpts from IGB publications:\n\n{context}\n\n"
            f"Please answer the question based on these sources. Cite each claim with [Source N]."
        )

        answer = _call_claude(
            [{"role": "user", "content": prompt}],
            system=system,
            max_tokens=1500,
        )

        if answer:
            # Fetch DOIs for sources
            conn = sqlite3.connect(DB_PATH)
            for src in sources:
                row = conn.execute(
                    "SELECT doi FROM publications WHERE id = ?",
                    (src["publication_id"],)
                ).fetchone()
                src["doi"] = row[0] if row else ""
            conn.close()

            return {
                "answer": answer,
                "sources": sources,
                "mode": "generative",
                "chunks_used": len(chunks),
            }

    # Extractive fallback — build structured answer from chunks
    answer_parts = []
    answer_parts.append(
        f"Based on {len(sources)} IGB publications, here's what I found:\n"
    )

    for chunk in chunks[:8]:
        pub_id = chunk["publication_id"]
        src_num = next(s["number"] for s in sources if s["publication_id"] == pub_id)
        title = chunk["title"]
        year = chunk.get("year", "")
        text = chunk["chunk_text"]

        # Extract key sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        key_sentences = []
        for s in sentences:
            s = s.strip()
            if len(s) > 40 and not s.lower().startswith(("this study", "in this", "here we", "we present")):
                key_sentences.append(s)
                if len(key_sentences) >= 2:
                    break

        if key_sentences:
            answer_parts.append(
                f"- {' '.join(key_sentences)} [Source {src_num}]"
            )

    answer_parts.append(
        "\n*This is an extractive answer built from publication text. "
        "Configure an Anthropic API key for AI-generated answers with deeper synthesis.*"
    )

    # Fetch DOIs
    conn = sqlite3.connect(DB_PATH)
    for src in sources:
        row = conn.execute(
            "SELECT doi FROM publications WHERE id = ?",
            (src["publication_id"],)
        ).fetchone()
        src["doi"] = row[0] if row else ""
    conn.close()

    return {
        "answer": "\n".join(answer_parts),
        "sources": sources,
        "mode": "extractive",
        "chunks_used": len(chunks),
    }
