"""Research Gap Finder — compare IGB publication coverage vs global trends."""

import os
import sqlite3
import time
import logging
import requests
from collections import Counter

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")
BASE_URL = "https://api.openalex.org"
MAILTO = "erika.freeman@igb-berlin.de"

# IGB's core research domains for comparison
IGB_DOMAINS = [
    "freshwater ecology",
    "limnology",
    "fish biology",
    "aquatic biodiversity",
    "water quality",
    "river ecology",
    "lake ecology",
    "plankton ecology",
    "aquatic biogeochemistry",
    "fisheries management",
    "eDNA metabarcoding",
    "microplastics freshwater",
    "urban water",
    "aquatic invasive species",
    "hydrological modeling",
    "climate change freshwater",
    "dissolved organic matter",
    "cyanobacteria blooms",
    "fish migration",
    "groundwater ecology",
    "wetland ecology",
    "aquatic microbiology",
    "freshwater conservation",
    "remote sensing water",
    "environmental DNA",
]


def get_igb_concept_coverage() -> dict:
    """Analyze what concepts IGB publishes on and their frequency."""
    conn = sqlite3.connect(DB_PATH)

    # Get concept frequencies
    rows = conn.execute("""
        SELECT concept_name, COUNT(*) as count, AVG(score) as avg_score
        FROM concepts
        GROUP BY concept_name
        ORDER BY count DESC
    """).fetchall()

    concepts = {}
    for name, count, avg_score in rows:
        concepts[name.lower()] = {
            "name": name,
            "count": count,
            "avg_score": round(avg_score, 3) if avg_score else 0,
        }

    # Get year distribution
    year_counts = {}
    for row in conn.execute(
        "SELECT year, COUNT(*) FROM publications WHERE year IS NOT NULL GROUP BY year ORDER BY year"
    ).fetchall():
        year_counts[row[0]] = row[1]

    # Recent vs older concept trends
    recent_concepts = Counter()
    older_concepts = Counter()
    for row in conn.execute("""
        SELECT c.concept_name, p.year
        FROM concepts c
        JOIN publications p ON c.publication_id = p.id
        WHERE p.year IS NOT NULL
    """).fetchall():
        name = row[0].lower()
        if row[1] >= 2020:
            recent_concepts[name] += 1
        elif row[1] < 2020:
            older_concepts[name] += 1

    # Find emerging vs declining topics at IGB
    emerging = []
    declining = []
    for concept in set(list(recent_concepts.keys()) + list(older_concepts.keys())):
        recent = recent_concepts.get(concept, 0)
        older = older_concepts.get(concept, 0)

        # Normalize by total papers in each period
        recent_total = sum(v for k, v in year_counts.items() if k >= 2020) or 1
        older_total = sum(v for k, v in year_counts.items() if k < 2020) or 1

        recent_rate = recent / recent_total
        older_rate = older / older_total

        if recent_rate > older_rate * 1.5 and recent >= 5:
            emerging.append({
                "name": concept,
                "recent_count": recent,
                "older_count": older,
                "growth": round((recent_rate - older_rate) / max(older_rate, 0.001) * 100),
            })
        elif older_rate > recent_rate * 1.5 and older >= 10:
            declining.append({
                "name": concept,
                "recent_count": recent,
                "older_count": older,
                "decline": round((older_rate - recent_rate) / max(older_rate, 0.001) * 100),
            })

    emerging.sort(key=lambda x: -x["growth"])
    declining.sort(key=lambda x: -x["decline"])

    conn.close()

    return {
        "concepts": concepts,
        "year_counts": year_counts,
        "total_concepts": len(concepts),
        "emerging": emerging[:20],
        "declining": declining[:20],
    }


def get_global_trends(domains: list = None) -> list:
    """Get global publication trends for freshwater research domains.

    Queries OpenAlex to see how many papers are published globally
    in each domain over recent years.
    """
    if domains is None:
        domains = IGB_DOMAINS

    results = []

    for domain in domains:
        try:
            # Get total count and recent trend
            url = (
                f"{BASE_URL}/works?"
                f"filter=default.search:{domain},"
                f"from_publication_date:2020-01-01"
                f"&select=id"
                f"&per_page=1"
                f"&mailto={MAILTO}"
            )

            resp = requests.get(url, timeout=15)
            if resp.status_code == 429:
                time.sleep(2)
                resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                continue

            data = resp.json()
            global_count = data.get("meta", {}).get("count", 0)

            results.append({
                "domain": domain,
                "global_count_since_2020": global_count,
            })

            time.sleep(0.15)

        except Exception as e:
            logger.debug(f"Trend check failed for {domain}: {e}")
            continue

    return results


def find_gaps(max_domains: int = 15) -> dict:
    """Compare IGB coverage vs global trends to find research gaps.

    Returns domains where global activity is high but IGB has few/no papers.
    """
    # Get IGB's coverage
    igb_data = get_igb_concept_coverage()
    igb_concepts = igb_data["concepts"]

    # Count IGB papers matching each domain (search in titles + abstracts)
    conn = sqlite3.connect(DB_PATH)
    igb_domain_counts = {}

    for domain in IGB_DOMAINS[:max_domains]:
        words = domain.split()
        # Use FTS if available, else LIKE
        try:
            fts_query = " AND ".join(f'"{w}"' for w in words if len(w) > 2)
            count = conn.execute(
                "SELECT COUNT(*) FROM publications_fts WHERE publications_fts MATCH ?",
                (fts_query,)
            ).fetchone()[0]
        except Exception:
            like_clause = " AND ".join(
                f"(title LIKE '%{w}%' OR abstract LIKE '%{w}%')" for w in words if len(w) > 2
            )
            if like_clause:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM publications WHERE {like_clause}"
                ).fetchone()[0]
            else:
                count = 0

        igb_domain_counts[domain] = count

    conn.close()

    # Get global trends
    global_trends = get_global_trends(IGB_DOMAINS[:max_domains])
    global_map = {t["domain"]: t["global_count_since_2020"] for t in global_trends}

    # Calculate gap scores
    gaps = []
    strengths = []
    opportunities = []

    for domain in IGB_DOMAINS[:max_domains]:
        igb_count = igb_domain_counts.get(domain, 0)
        global_count = global_map.get(domain, 0)

        if global_count == 0:
            continue

        # IGB's share of global output
        igb_share = igb_count / max(global_count, 1) * 100

        entry = {
            "domain": domain,
            "igb_count": igb_count,
            "global_count": global_count,
            "igb_share_pct": round(igb_share, 3),
        }

        if igb_count <= 5 and global_count > 1000:
            entry["gap_type"] = "underrepresented"
            entry["recommendation"] = f"High global activity ({global_count:,} papers since 2020) but only {igb_count} IGB papers. Potential growth area."
            gaps.append(entry)
        elif igb_share > 0.5:
            entry["gap_type"] = "strength"
            entry["recommendation"] = f"IGB is well-represented ({igb_count} papers, {igb_share:.1f}% of global output)."
            strengths.append(entry)
        elif igb_count > 10 and global_count > 5000:
            entry["gap_type"] = "opportunity"
            entry["recommendation"] = f"Growing field globally ({global_count:,} papers). IGB has {igb_count} papers — room to increase impact."
            opportunities.append(entry)
        else:
            opportunities.append(entry)

    gaps.sort(key=lambda x: -x["global_count"])
    strengths.sort(key=lambda x: -x["igb_share_pct"])
    opportunities.sort(key=lambda x: -x["global_count"])

    return {
        "gaps": gaps,
        "strengths": strengths,
        "opportunities": opportunities,
        "igb_coverage": igb_data,
    }
