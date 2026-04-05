"""Relevance scoring and explanation generation for discovered papers."""

import json
from collections import defaultdict
from datetime import datetime, timedelta


def _compute_recency_boost(pub_date: str) -> float:
    """Compute a 0-1 recency boost that decays over time.

    - Published today: 1.0
    - 3 days ago: ~0.75
    - 7 days ago: ~0.5
    - 14 days ago: ~0.25
    - 30+ days ago: ~0.05
    """
    if not pub_date:
        return 0.3  # Unknown date gets a moderate default

    try:
        pub_dt = datetime.strptime(pub_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return 0.3

    days_old = (datetime.now() - pub_dt).days
    if days_old < 0:
        days_old = 0  # Future date (pre-print), treat as brand new

    # Exponential decay: half-life of ~7 days
    import math
    return max(0.05, math.exp(-0.1 * days_old))


def _compute_citation_proximity(paper: dict, igb_pub_ids: set) -> float:
    """Check if paper references any IGB publications.

    Returns 1.0 if it cites IGB work, 0.0 otherwise.
    Uses the referenced_works field from OpenAlex if available.
    """
    # Check if paper was fetched as citing IGB (source marker)
    if paper.get("source") == "openalex_citing":
        return 1.0

    if not igb_pub_ids:
        return 0.0

    # Check referenced_works if available (OpenAlex provides this)
    refs = paper.get("referenced_works") or []
    for ref in refs:
        ref_str = str(ref).lower()
        for igb_id in igb_pub_ids:
            if igb_id.lower() in ref_str:
                return 1.0

    return 0.0


def score_paper(paper: dict, profile: dict, department: str = "all") -> dict:
    """Compute relevance score for a paper against the IGB profile.

    Returns dict with:
        relevance_score: 0-1 composite score
        semantic_score: placeholder for embedding-based score (0 until embeddings built)
        concept_overlap_score: concept overlap with IGB profile
        explanation: human-readable relevance explanation
    """
    # Concept overlap scoring
    paper_concepts = {c["concept_name"].lower() for c in paper.get("concepts", [])}
    paper_concept_scores = {c["concept_name"].lower(): c.get("score", 0) for c in paper.get("concepts", [])}

    if department == "all":
        igb_concepts = {c["concept_name"].lower(): c for c in profile.get("institute_concepts", [])}
    else:
        dept_concepts = profile.get("department_concepts", {}).get(department, [])
        igb_concepts = {c["concept_name"].lower(): c for c in dept_concepts}

    if not igb_concepts:
        igb_concepts = {c["concept_name"].lower(): c for c in profile.get("institute_concepts", [])}

    # Weighted overlap: concepts shared, weighted by IGB frequency
    overlap = paper_concepts & set(igb_concepts.keys())
    if not overlap or not igb_concepts:
        concept_overlap = 0.0
    else:
        # Weight by how important each concept is to IGB (by paper_count or count)
        max_count = max(
            (c.get("paper_count") or c.get("count") or 1) for c in igb_concepts.values()
        )
        weighted_overlap = sum(
            (igb_concepts[c].get("paper_count") or igb_concepts[c].get("count") or 1) / max_count
            for c in overlap
        )
        concept_overlap = min(weighted_overlap / 3.0, 1.0)  # Normalize: 3+ strong overlaps = 1.0

    # Dynamic recency boost based on publication date
    recency_boost = _compute_recency_boost(paper.get("publication_date", ""))

    # Citation proximity: does this paper cite IGB work?
    igb_pub_ids = set(profile.get("igb_pub_ids", []))
    citation_proximity = _compute_citation_proximity(paper, igb_pub_ids)

    # Journal quality boost
    from . import models as _m
    journal_boost = _m._journal_boost(paper.get("journal", ""))

    # Composite score: concept_overlap=0.5, journal=0.2, recency=0.15, citation=0.15
    relevance_score = (
        0.5 * concept_overlap +
        0.2 * journal_boost +
        0.15 * recency_boost +
        0.15 * citation_proximity
    )

    # Generate explanation
    explanation = _generate_explanation(
        paper, overlap, igb_concepts, department, concept_overlap,
        journal_boost, citation_proximity, recency_boost
    )

    return {
        "relevance_score": round(relevance_score, 3),
        "semantic_score": 0.0,
        "concept_overlap_score": round(concept_overlap, 3),
        "explanation": explanation,
    }


def _generate_explanation(paper, overlap, igb_concepts, department, concept_overlap,
                          journal_boost=0, citation_proximity=0, recency_boost=0.5):
    """Generate a human-readable relevance explanation."""
    parts = []

    if overlap:
        top_shared = sorted(
            overlap,
            key=lambda c: igb_concepts.get(c, {}).get("paper_count", igb_concepts.get(c, {}).get("count", 0)),
            reverse=True,
        )[:3]
        formatted = ", ".join(f"**{c.title()}**" for c in top_shared)

        if department == "all":
            parts.append(f"Shares key concepts with IGB research: {formatted}.")
        else:
            dept_short = department.split(")")[0] + ")" if ")" in department else department
            parts.append(f"Connects to {dept_short} through {formatted}.")

    # Citation proximity signal
    if citation_proximity > 0:
        parts.append("**Cites IGB publications** — direct connection to institute research.")

    journal = paper.get("journal", "")
    if journal:
        if journal_boost >= 0.9:
            parts.append(f"Published in **{journal}** (top-tier journal).")
        elif journal_boost >= 0.75:
            parts.append(f"Published in **{journal}** (high-impact journal).")
        else:
            parts.append(f"Published in {journal}.")

    if concept_overlap >= 0.7:
        parts.append("Strong alignment with IGB's research profile.")
    elif concept_overlap >= 0.4:
        parts.append("Moderate overlap with IGB's core research areas.")
    elif overlap:
        parts.append("Some thematic connection to IGB's work.")

    # Recency note for very fresh papers
    if recency_boost >= 0.9:
        parts.append("Published in the last few days.")

    if not parts:
        parts.append("Potentially relevant to IGB's broader research interests.")

    return " ".join(parts)


def score_papers_batch(papers: list, profile: dict) -> list:
    """Score a batch of papers against all departments + institute-wide.

    Returns list of (paper_id, department, score_dict) tuples.
    """
    results = []

    departments = list(profile.get("department_concepts", {}).keys())

    for paper in papers:
        # Institute-wide score
        score = score_paper(paper, profile, department="all")
        results.append((paper["id"], "all", score))

        # Per-department scores
        for dept in departments:
            dept_score = score_paper(paper, profile, department=dept)
            if dept_score["relevance_score"] > 0.2:  # Only store if somewhat relevant
                results.append((paper["id"], dept, dept_score))

    return results
