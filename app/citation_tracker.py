"""Citation tracking — who cited an IGB paper, and co-citation analysis."""

import time
import sqlite3
import os
import requests
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openalex.org"
MAILTO = "erika.freeman@igb-berlin.de"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


def get_citing_papers(pub_id: str, limit: int = 50) -> dict:
    """Find papers that cite a given IGB publication.

    Returns dict with:
        - citing_papers: list of dicts with title, authors, year, journal, doi, is_igb
        - stats: total_citations, igb_self_citations, top_citing_journal, citing_years
    """
    # Extract short ID
    short_id = pub_id.split("/")[-1] if "openalex.org/" in pub_id else pub_id

    # Query OpenAlex for works that cite this paper
    url = (
        f"{BASE_URL}/works?"
        f"filter=cites:{short_id}"
        f"&select=id,title,authorships,publication_year,primary_location,doi,cited_by_count,type"
        f"&sort=publication_year:desc"
        f"&per_page={limit}"
        f"&mailto={MAILTO}"
    )

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(url, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"OpenAlex citing query failed: {resp.status_code}")
            return {"citing_papers": [], "stats": {}}

        data = resp.json()
    except Exception as e:
        logger.error(f"OpenAlex citing query error: {e}")
        return {"citing_papers": [], "stats": {}}

    total_count = data.get("meta", {}).get("count", 0)

    # Load IGB author IDs for self-citation detection
    igb_author_ids = set()
    try:
        conn = sqlite3.connect(DB_PATH)
        for row in conn.execute("SELECT id FROM authors WHERE is_current_staff = 1"):
            igb_author_ids.add(row[0])
        conn.close()
    except Exception:
        pass

    # Load IGB publication IDs
    igb_pub_ids = set()
    try:
        conn = sqlite3.connect(DB_PATH)
        for row in conn.execute("SELECT id FROM publications"):
            igb_pub_ids.add(row[0])
        conn.close()
    except Exception:
        pass

    citing_papers = []
    journal_counts = {}
    year_counts = {}
    igb_self_count = 0

    for work in data.get("results", []):
        work_id = work.get("id", "")
        title = work.get("title", "Untitled")
        year = work.get("publication_year")
        doi = work.get("doi", "")
        cited_by = work.get("cited_by_count", 0)

        # Get journal
        loc = work.get("primary_location") or {}
        source = loc.get("source") or {}
        journal = source.get("display_name", "")

        # Get authors
        authors = []
        is_igb = work_id in igb_pub_ids
        for auth in (work.get("authorships") or [])[:5]:
            name = (auth.get("author") or {}).get("display_name", "")
            auth_id = (auth.get("author") or {}).get("id", "")
            if auth_id in igb_author_ids:
                is_igb = True
            if name:
                authors.append(name)

        if is_igb:
            igb_self_count += 1

        citing_papers.append({
            "id": work_id,
            "title": title,
            "authors": authors,
            "authors_str": ", ".join(authors[:3]) + (" et al." if len(authors) > 3 else ""),
            "year": year,
            "journal": journal,
            "doi": doi,
            "cited_by_count": cited_by,
            "is_igb": is_igb,
            "in_feuerstein": work_id in igb_pub_ids,
        })

        if journal:
            journal_counts[journal] = journal_counts.get(journal, 0) + 1
        if year:
            year_counts[year] = year_counts.get(year, 0) + 1

    # Build stats
    stats = {
        "total_citations": total_count,
        "shown": len(citing_papers),
        "igb_self_citations": igb_self_count,
        "external_citations": len(citing_papers) - igb_self_count,
        "top_citing_journals": sorted(journal_counts.items(), key=lambda x: -x[1])[:5],
        "citing_years": dict(sorted(year_counts.items())),
    }

    return {"citing_papers": citing_papers, "stats": stats}


def get_co_cited_papers(pub_id: str, limit: int = 20) -> list:
    """Find papers that are frequently co-cited with this paper.

    Papers that appear together in reference lists of other works.
    """
    short_id = pub_id.split("/")[-1] if "openalex.org/" in pub_id else pub_id

    # Get papers that cite this work
    url = (
        f"{BASE_URL}/works?"
        f"filter=cites:{short_id}"
        f"&select=id,referenced_works"
        f"&per_page=50"
        f"&mailto={MAILTO}"
    )

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []

    # Count how often each reference appears alongside our paper
    co_ref_counts = {}
    for work in data.get("results", []):
        refs = work.get("referenced_works") or []
        for ref_id in refs:
            if ref_id != pub_id:
                co_ref_counts[ref_id] = co_ref_counts.get(ref_id, 0) + 1

    if not co_ref_counts:
        return []

    # Get top co-cited works
    top_co_cited = sorted(co_ref_counts.items(), key=lambda x: -x[1])[:limit]

    # Fetch metadata for these works
    short_ids = [pid.split("/")[-1] for pid, _ in top_co_cited]
    co_cited_meta = {}

    for i in range(0, len(short_ids), 50):
        batch = short_ids[i:i+50]
        filter_str = "|".join(batch)
        meta_url = (
            f"{BASE_URL}/works?"
            f"filter=openalex:{filter_str}"
            f"&select=id,title,publication_year,doi,authorships,primary_location,cited_by_count"
            f"&per_page=50"
            f"&mailto={MAILTO}"
        )

        try:
            resp = requests.get(meta_url, timeout=15)
            if resp.status_code == 200:
                for w in resp.json().get("results", []):
                    wid = w.get("id", "")
                    authors = []
                    for a in (w.get("authorships") or [])[:3]:
                        name = (a.get("author") or {}).get("display_name", "")
                        if name:
                            authors.append(name)

                    loc = w.get("primary_location") or {}
                    source = loc.get("source") or {}

                    co_cited_meta[wid] = {
                        "title": w.get("title", ""),
                        "year": w.get("publication_year"),
                        "doi": w.get("doi", ""),
                        "authors_str": ", ".join(authors) + (" et al." if len(w.get("authorships") or []) > 3 else ""),
                        "journal": source.get("display_name", ""),
                        "cited_by_count": w.get("cited_by_count", 0),
                    }
            time.sleep(0.2)
        except Exception:
            continue

    results = []
    for pid, count in top_co_cited:
        meta = co_cited_meta.get(pid, {})
        if meta.get("title"):
            results.append({
                "id": pid,
                "co_citation_count": count,
                **meta,
            })

    return results[:limit]
