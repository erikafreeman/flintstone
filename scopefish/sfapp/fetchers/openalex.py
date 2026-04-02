"""Fetch recent papers from OpenAlex by IGB research concepts."""

import time
import requests

BASE_URL = "https://api.openalex.org"
IGB_ID = "I4210116314"
MAILTO = "erika.freeman@igb-berlin.de"


def reconstruct_abstract(inverted_index: dict) -> str:
    """Reconstruct abstract from OpenAlex inverted index format."""
    if not inverted_index:
        return ""
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)


def fetch_recent_by_concepts(
    concept_names: list,
    date_from: str,
    date_to: str,
    max_results: int = 500,
) -> list:
    """Fetch recent papers matching IGB's top concepts.

    Uses OpenAlex topic/concept search with date filtering.
    Excludes IGB's own papers.

    Args:
        concept_names: list of concept names to search for
        date_from: ISO date string (YYYY-MM-DD)
        date_to: ISO date string (YYYY-MM-DD)
        max_results: maximum papers to fetch

    Returns:
        list of paper dicts ready for storage
    """
    all_papers = {}

    # Build concept search queries in batches of 5
    for i in range(0, min(len(concept_names), 50), 5):
        batch = concept_names[i:i + 5]
        search_terms = " OR ".join(batch)

        cursor = "*"
        batch_count = 0

        while cursor and batch_count < max_results // 10:
            url = (
                f"{BASE_URL}/works?"
                f"search={requests.utils.quote(search_terms)}"
                f"&filter=from_publication_date:{date_from},"
                f"to_publication_date:{date_to},"
                f"institutions.id:!{IGB_ID},"
                f"type:article|review|preprint"
                f"&per_page=50"
                f"&cursor={cursor}"
                f"&mailto={MAILTO}"
                f"&select=id,title,abstract_inverted_index,publication_year,"
                f"publication_date,doi,primary_location,cited_by_count,"
                f"type,open_access,authorships,concepts"
            )

            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  OpenAlex error for '{batch[0]}...': {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for work in results:
                work_id = work.get("id", "")
                if work_id in all_papers:
                    continue

                abstract_inv = work.get("abstract_inverted_index")
                abstract = reconstruct_abstract(abstract_inv) if abstract_inv else ""

                primary_loc = work.get("primary_location") or {}
                source = primary_loc.get("source") or {}
                journal = source.get("display_name", "")

                oa = work.get("open_access") or {}
                oa_url = oa.get("oa_url", "")

                # Extract authors
                authors = []
                for idx, authorship in enumerate(work.get("authorships", [])):
                    author = authorship.get("author") or {}
                    institutions = authorship.get("institutions") or []
                    inst_name = institutions[0].get("display_name", "") if institutions else ""
                    authors.append({
                        "author_name": author.get("display_name", ""),
                        "author_id": author.get("id", ""),
                        "institution": inst_name,
                        "position": idx + 1,
                    })

                # Extract concepts
                concepts = []
                for concept in work.get("concepts", []):
                    if concept.get("display_name") and concept.get("score", 0) > 0:
                        concepts.append({
                            "concept_name": concept["display_name"],
                            "score": concept.get("score", 0),
                        })

                all_papers[work_id] = {
                    "id": work_id,
                    "source": "openalex",
                    "title": work.get("title", ""),
                    "abstract": abstract,
                    "year": work.get("publication_year"),
                    "publication_date": work.get("publication_date", ""),
                    "doi": work.get("doi", ""),
                    "journal": journal,
                    "cited_by_count": work.get("cited_by_count", 0),
                    "type": work.get("type", ""),
                    "open_access_url": oa_url,
                    "authors": authors,
                    "concepts": concepts,
                }

            batch_count += 1
            cursor = data.get("meta", {}).get("next_cursor")
            time.sleep(0.1)

            if len(all_papers) >= max_results:
                break

        if len(all_papers) >= max_results:
            break

    print(f"  OpenAlex: fetched {len(all_papers)} papers")
    return list(all_papers.values())


def fetch_citing_igb(igb_pub_ids: list, date_from: str, date_to: str, max_results: int = 100) -> list:
    """Fetch recent papers that cite IGB publications."""
    all_papers = {}

    # Take a sample of recent IGB papers to check citations for
    sample_ids = igb_pub_ids[:50]

    for pub_id in sample_ids:
        # Extract the OpenAlex work ID (W number)
        oa_id = pub_id.replace("https://openalex.org/", "")

        url = (
            f"{BASE_URL}/works?"
            f"filter=cites:{oa_id},"
            f"from_publication_date:{date_from},"
            f"to_publication_date:{date_to}"
            f"&per_page=10"
            f"&mailto={MAILTO}"
            f"&select=id,title,abstract_inverted_index,publication_year,"
            f"publication_date,doi,primary_location,cited_by_count,"
            f"type,open_access,authorships,concepts"
        )

        try:
            resp = requests.get(url, timeout=20)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception:
            continue

        for work in data.get("results", []):
            work_id = work.get("id", "")
            if work_id in all_papers:
                continue

            abstract_inv = work.get("abstract_inverted_index")
            abstract = reconstruct_abstract(abstract_inv) if abstract_inv else ""

            primary_loc = work.get("primary_location") or {}
            source = primary_loc.get("source") or {}

            oa = work.get("open_access") or {}

            authors = []
            for idx, authorship in enumerate(work.get("authorships", [])):
                author = authorship.get("author") or {}
                institutions = authorship.get("institutions") or []
                inst_name = institutions[0].get("display_name", "") if institutions else ""
                authors.append({
                    "author_name": author.get("display_name", ""),
                    "author_id": author.get("id", ""),
                    "institution": inst_name,
                    "position": idx + 1,
                })

            concepts = []
            for concept in work.get("concepts", []):
                if concept.get("display_name") and concept.get("score", 0) > 0:
                    concepts.append({
                        "concept_name": concept["display_name"],
                        "score": concept.get("score", 0),
                    })

            all_papers[work_id] = {
                "id": work_id,
                "source": "openalex",
                "title": work.get("title", ""),
                "abstract": abstract,
                "year": work.get("publication_year"),
                "publication_date": work.get("publication_date", ""),
                "doi": work.get("doi", ""),
                "journal": source.get("display_name", ""),
                "cited_by_count": work.get("cited_by_count", 0),
                "type": work.get("type", ""),
                "open_access_url": oa.get("oa_url", ""),
                "authors": authors,
                "concepts": concepts,
            }

        time.sleep(0.1)
        if len(all_papers) >= max_results:
            break

    print(f"  OpenAlex citations: fetched {len(all_papers)} papers citing IGB work")
    return list(all_papers.values())
