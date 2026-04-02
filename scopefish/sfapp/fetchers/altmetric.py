"""Fetch Altmetric attention data for papers.

Altmetric tracks mentions across news, blogs, X/Twitter, policy documents,
Wikipedia, and more. The free API provides attention scores and counts.
"""

import time
import requests

ALTMETRIC_API = "https://api.altmetric.com/v1"


def fetch_for_doi(doi: str) -> dict | None:
    """Fetch Altmetric data for a paper by DOI.

    Args:
        doi: Full DOI URL or bare DOI (e.g. "10.1234/abcd")

    Returns:
        dict with altmetric_score, news_count, blog_count, twitter_count,
        policy_count, news_sources, or None if not found
    """
    # Clean DOI
    doi_bare = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    if not doi_bare:
        return None

    try:
        resp = requests.get(
            f"{ALTMETRIC_API}/doi/{doi_bare}",
            timeout=10,
        )
        if resp.status_code == 404:
            return None  # Paper not tracked by Altmetric
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception:
        return None

    # Extract counts from response
    context = data.get("context", {})
    cited_by = data.get("cited_by_tweeters_count", 0)
    news_count = data.get("cited_by_msm_count", 0)
    blog_count = data.get("cited_by_feeds_count", 0)
    policy_count = data.get("cited_by_policies_count", 0)

    # News source names
    news_sources = []
    for source in data.get("cited_by_msm", [])[:5]:
        name = source.get("name") or source.get("title", "")
        if name:
            news_sources.append(name)

    return {
        "altmetric_score": data.get("score", 0),
        "news_count": news_count,
        "blog_count": blog_count,
        "twitter_count": cited_by,
        "policy_count": policy_count,
        "news_sources": ", ".join(news_sources),
    }


def fetch_for_papers(papers: list) -> dict:
    """Fetch Altmetric data for a batch of papers.

    Args:
        papers: list of paper dicts (need 'id' and 'doi')

    Returns:
        dict mapping paper_id -> altmetric data dict
    """
    results = {}
    fetched = 0

    for paper in papers:
        doi = paper.get("doi") or ""
        if not doi:
            continue

        data = fetch_for_doi(doi)
        if data and data["altmetric_score"] > 0:
            results[paper["id"]] = data
            fetched += 1

        time.sleep(0.2)  # Altmetric rate limit: ~5 req/sec

        if fetched >= 50:
            break

    print(f"  Altmetric: found data for {len(results)} papers")
    return results
