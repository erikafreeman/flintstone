"""Weekly digest generation logic."""

from datetime import datetime


def current_week_string() -> str:
    """Return current ISO week string like '2026-W14'."""
    now = datetime.now()
    return now.strftime("%G-W%V")


def group_by_tier(papers: list) -> dict:
    """Group papers into relevance tiers.

    Returns dict with keys: high, medium, notable
    """
    high = []
    medium = []
    notable = []

    for paper in papers:
        score = paper.get("relevance_score", 0)
        if score >= 0.7:
            high.append(paper)
        elif score >= 0.4:
            medium.append(paper)
        else:
            notable.append(paper)

    return {
        "high": high,
        "medium": medium,
        "notable": notable,
    }


def digest_summary(papers: list) -> dict:
    """Generate summary stats for a digest."""
    tiers = group_by_tier(papers)
    journals = set()
    for p in papers:
        if p.get("journal"):
            journals.add(p["journal"])

    return {
        "total": len(papers),
        "high_count": len(tiers["high"]),
        "medium_count": len(tiers["medium"]),
        "notable_count": len(tiers["notable"]),
        "unique_journals": len(journals),
    }
