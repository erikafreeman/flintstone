"""Fetch recent papers from CrossRef API."""

import time
import requests

CR_BASE = "https://api.crossref.org/works"
MAILTO = "erika.freeman@igb-berlin.de"


def fetch_recent(query_terms: list, date_from: str, max_results: int = 50) -> list:
    """Search CrossRef for recent papers matching IGB concepts.

    Args:
        query_terms: list of search terms
        date_from: ISO date string (YYYY-MM-DD)
        max_results: max papers to return

    Returns:
        list of paper dicts
    """
    all_papers = {}

    for term in query_terms[:5]:
        params = {
            "query": term,
            "filter": f"from-pub-date:{date_from},type:journal-article",
            "rows": min(20, max_results),
            "sort": "published",
            "order": "desc",
            "mailto": MAILTO,
        }

        try:
            resp = requests.get(CR_BASE, params=params, timeout=20)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception as e:
            print(f"  CrossRef error for '{term}': {e}")
            continue

        for item in data.get("message", {}).get("items", []):
            doi = item.get("DOI", "")
            if not doi or doi in all_papers:
                continue

            title_parts = item.get("title", [])
            title = title_parts[0] if title_parts else ""

            abstract = item.get("abstract", "")
            # Strip HTML from CrossRef abstracts
            if abstract:
                import re
                abstract = re.sub(r"<[^>]+>", "", abstract)

            pub_date_parts = item.get("published", {}).get("date-parts", [[]])
            year = pub_date_parts[0][0] if pub_date_parts and pub_date_parts[0] else None
            pub_date = "-".join(str(p).zfill(2) for p in pub_date_parts[0]) if pub_date_parts and pub_date_parts[0] else ""

            journal = (item.get("container-title") or [""])[0]

            authors = []
            for idx, author in enumerate(item.get("author", [])):
                name = f"{author.get('given', '')} {author.get('family', '')}".strip()
                affil = (author.get("affiliation") or [{}])[0].get("name", "") if author.get("affiliation") else ""
                authors.append({
                    "author_name": name,
                    "author_id": "",
                    "institution": affil,
                    "position": idx + 1,
                })

            all_papers[doi] = {
                "id": f"cr:{doi}",
                "source": "crossref",
                "title": title,
                "abstract": abstract,
                "year": year,
                "publication_date": pub_date,
                "doi": f"https://doi.org/{doi}",
                "journal": journal,
                "cited_by_count": item.get("is-referenced-by-count", 0),
                "type": item.get("type", "article"),
                "open_access_url": "",
                "authors": authors,
                "concepts": [],
            }

        time.sleep(0.5)
        if len(all_papers) >= max_results:
            break

    print(f"  CrossRef: fetched {len(all_papers)} papers")
    return list(all_papers.values())
