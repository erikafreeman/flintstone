"""Citation network analysis for search results."""

import time
import requests

BASE_URL = "https://api.openalex.org"
MAILTO = "erika.freeman@igb-berlin.de"


def fetch_citation_links(pub_ids: list) -> dict:
    """Fetch which of the given publications cite each other.

    Returns dict with:
        - nodes: list of {id, title, year}
        - edges: list of {source, target} (source cites target)
        - stats: {total_edges, most_cited_id, most_citing_id}
    """
    if not pub_ids or len(pub_ids) < 2:
        return {"nodes": [], "edges": [], "stats": {}}

    # OpenAlex IDs are URLs like "https://openalex.org/W1234"
    pub_id_set = set(pub_ids)

    edges = []
    ref_counts = {}  # how many times each paper is cited by others in the set
    citing_counts = {}  # how many papers each paper cites within the set

    # Fetch referenced_works for each publication (batch via API)
    # Use the filter API to get all at once
    short_ids = []
    for pid in pub_ids:
        # Extract the short ID (W1234567)
        if "openalex.org/" in pid:
            short_ids.append(pid.split("/")[-1])
        else:
            short_ids.append(pid)

    # Fetch in batches of 50 using the pipe-separated filter
    for i in range(0, len(short_ids), 50):
        batch = short_ids[i:i+50]
        filter_str = "|".join(batch)
        url = (
            f"{BASE_URL}/works?"
            f"filter=openalex:{filter_str}"
            f"&select=id,referenced_works"
            f"&per_page=50"
            f"&mailto={MAILTO}"
        )

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 429:
                time.sleep(2)
                resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception:
            continue

        for work in data.get("results", []):
            work_id = work.get("id", "")
            refs = work.get("referenced_works", []) or []

            for ref_id in refs:
                if ref_id in pub_id_set and ref_id != work_id:
                    edges.append({"source": work_id, "target": ref_id})
                    ref_counts[ref_id] = ref_counts.get(ref_id, 0) + 1
                    citing_counts[work_id] = citing_counts.get(work_id, 0) + 1

        time.sleep(0.2)

    stats = {}
    if ref_counts:
        most_cited = max(ref_counts, key=ref_counts.get)
        stats["most_cited_id"] = most_cited
        stats["most_cited_count"] = ref_counts[most_cited]
    if citing_counts:
        most_citing = max(citing_counts, key=citing_counts.get)
        stats["most_citing_id"] = most_citing
        stats["most_citing_count"] = citing_counts[most_citing]
    stats["total_edges"] = len(edges)

    return {
        "edges": edges,
        "stats": stats,
        "ref_counts": ref_counts,
    }
