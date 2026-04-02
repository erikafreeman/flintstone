"""Fetch social buzz from Bluesky (AT Protocol) about discovered papers."""

import time
import requests

BSKY_API = "https://public.api.bsky.app"


def search_paper_mentions(doi: str = None, title: str = None, max_results: int = 10) -> list:
    """Search Bluesky for posts mentioning a paper by DOI or title keywords.

    Args:
        doi: Paper DOI URL (e.g. https://doi.org/10.1234/...)
        title: Paper title (will use first 5 significant words)
        max_results: Max posts to return

    Returns:
        list of post dicts with: post_uri, author_handle, author_name, text, likes, reposts, posted_at
    """
    queries = []

    if doi:
        # Search for DOI (strip https://doi.org/ prefix)
        doi_short = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        queries.append(doi_short)

    if title:
        # Use first few significant words from title
        words = [w for w in title.split() if len(w) > 3 and w.lower() not in
                 {"from", "with", "that", "this", "their", "about", "between", "through"}]
        if len(words) >= 3:
            queries.append(" ".join(words[:5]))

    all_posts = {}
    for query in queries:
        try:
            resp = requests.get(
                f"{BSKY_API}/xrpc/app.bsky.feed.searchPosts",
                params={"q": query, "limit": min(max_results, 25)},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception as e:
            print(f"  Bluesky search error: {e}")
            continue

        for post in data.get("posts", []):
            uri = post.get("uri", "")
            if uri in all_posts:
                continue

            author = post.get("author", {})
            record = post.get("record", {})
            text = record.get("text", "")

            # Only include if text mentions the paper
            if doi and doi_short.lower() not in text.lower():
                if title and not any(w.lower() in text.lower() for w in (title.split()[:3] if title else [])):
                    continue

            all_posts[uri] = {
                "post_uri": uri,
                "platform": "bluesky",
                "author_handle": author.get("handle", ""),
                "author_name": author.get("displayName", author.get("handle", "")),
                "text": text[:500],
                "likes": post.get("likeCount", 0),
                "reposts": post.get("repostCount", 0),
                "posted_at": record.get("createdAt", ""),
            }

        time.sleep(0.3)  # Rate limit

    posts = sorted(all_posts.values(), key=lambda p: p.get("likes", 0), reverse=True)
    return posts[:max_results]


def fetch_social_for_papers(papers: list, max_per_paper: int = 5) -> dict:
    """Fetch Bluesky mentions for a batch of papers.

    Args:
        papers: list of paper dicts (need 'id', 'doi', 'title')
        max_per_paper: max posts per paper

    Returns:
        dict mapping paper_id -> list of post dicts
    """
    results = {}
    for paper in papers:
        doi = paper.get("doi") or ""
        title = paper.get("title") or ""
        if not doi and not title:
            continue

        posts = search_paper_mentions(doi=doi, title=title, max_results=max_per_paper)
        if posts:
            results[paper["id"]] = posts
            print(f"  Bluesky: {len(posts)} posts for '{title[:50]}...'")

        time.sleep(0.5)

    return results
