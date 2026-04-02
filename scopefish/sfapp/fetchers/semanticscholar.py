"""Fetch recent papers from Semantic Scholar API."""

import time
import requests

S2_BASE = "https://api.semanticscholar.org/graph/v1"
FIELDS = "paperId,title,abstract,year,publicationDate,externalIds,venue,citationCount,publicationTypes,authors"


def fetch_recent(query_terms: list, max_results: int = 100) -> list:
    """Search Semantic Scholar for recent papers matching IGB concepts.

    Args:
        query_terms: list of search terms
        max_results: max papers to return

    Returns:
        list of paper dicts
    """
    all_papers = {}

    for term in query_terms[:10]:
        url = f"{S2_BASE}/paper/search"
        params = {
            "query": term,
            "limit": min(20, max_results),
            "fields": FIELDS,
            "year": "2025-2026",
        }

        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                time.sleep(5)
                continue
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception as e:
            print(f"  S2 error for '{term}': {e}")
            continue

        for paper in data.get("data", []):
            paper_id = paper.get("paperId", "")
            if not paper_id or paper_id in all_papers:
                continue

            ext_ids = paper.get("externalIds") or {}
            doi = ext_ids.get("DOI", "")
            if doi and not doi.startswith("http"):
                doi = f"https://doi.org/{doi}"

            authors = []
            for idx, author in enumerate(paper.get("authors", [])):
                authors.append({
                    "author_name": author.get("name", ""),
                    "author_id": author.get("authorId", ""),
                    "institution": "",
                    "position": idx + 1,
                })

            all_papers[paper_id] = {
                "id": f"s2:{paper_id}",
                "source": "semantic_scholar",
                "title": paper.get("title", ""),
                "abstract": paper.get("abstract") or "",
                "year": paper.get("year"),
                "publication_date": paper.get("publicationDate") or "",
                "doi": doi,
                "journal": paper.get("venue") or "",
                "cited_by_count": paper.get("citationCount", 0),
                "type": (paper.get("publicationTypes") or ["article"])[0] if paper.get("publicationTypes") else "article",
                "open_access_url": "",
                "authors": authors,
                "concepts": [],
            }

        time.sleep(1)  # S2 rate limit: 1 req/sec without API key
        if len(all_papers) >= max_results:
            break

    print(f"  Semantic Scholar: fetched {len(all_papers)} papers")
    return list(all_papers.values())
