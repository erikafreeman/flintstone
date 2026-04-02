"""Relevance scoring and explanation generation for discovered papers."""

import json
from collections import defaultdict


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

    # Recency boost
    pub_date = paper.get("publication_date", "")
    recency_boost = 0.5  # Default moderate

    # Citation proximity (placeholder - would check if paper cites IGB work)
    citation_proximity = 0.0

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
    explanation = _generate_explanation(paper, overlap, igb_concepts, department, concept_overlap, journal_boost)

    return {
        "relevance_score": round(relevance_score, 3),
        "semantic_score": 0.0,
        "concept_overlap_score": round(concept_overlap, 3),
        "explanation": explanation,
    }


def _generate_explanation(paper, overlap, igb_concepts, department, concept_overlap, journal_boost=0):
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
