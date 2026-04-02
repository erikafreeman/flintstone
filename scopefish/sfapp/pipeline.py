"""Pipeline orchestrator: fetch -> dedupe -> score -> store."""

import re
import sqlite3
from datetime import datetime, timedelta

from . import models
from .igb_profile import load_profile
from .relevance import score_papers_batch
from .digest import current_week_string
from .fetchers import openalex, semanticscholar, crossref


def run(days: int = 7, max_papers: int = 500):
    """Run the full pipeline: fetch recent papers, score, and store.

    Args:
        days: how many days back to fetch
        max_papers: max papers to fetch per source
    """
    now = datetime.now()
    date_from = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    week_str = current_week_string()

    print(f"=== Scopefish Pipeline ===")
    print(f"Period: {date_from} to {date_to} (week {week_str})")

    # Load IGB profile
    profile = load_profile()
    if not profile["top_concept_names"]:
        print("ERROR: No IGB profile found. Run scripts/build_profile.py first.")
        return

    top_concepts = profile["top_concept_names"]
    print(f"IGB profile: {len(top_concepts)} concepts loaded")

    # Step 1: Fetch from all sources
    print("\n--- Fetching papers ---")
    all_papers = {}

    # OpenAlex: by concept
    oa_papers = openalex.fetch_recent_by_concepts(
        top_concepts[:30], date_from, date_to, max_results=max_papers
    )
    for p in oa_papers:
        all_papers[p["id"]] = p

    # OpenAlex: papers citing IGB
    if profile.get("igb_pub_ids"):
        citing = openalex.fetch_citing_igb(
            profile["igb_pub_ids"][:30], date_from, date_to, max_results=100
        )
        for p in citing:
            if p["id"] not in all_papers:
                all_papers[p["id"]] = p

    # Semantic Scholar
    s2_terms = top_concepts[:5]
    try:
        s2_papers = semanticscholar.fetch_recent(s2_terms, max_results=100)
        for p in s2_papers:
            if p["id"] not in all_papers:
                all_papers[p["id"]] = p
    except Exception as e:
        print(f"  S2 fetch failed: {e}")

    # CrossRef
    cr_terms = [
        "freshwater ecology",
        "dissolved organic matter lakes",
        "aquatic biodiversity",
        "fish population dynamics",
        "plankton microbial ecology",
    ]
    try:
        cr_papers = crossref.fetch_recent(cr_terms, date_from, max_results=50)
        for p in cr_papers:
            if p["id"] not in all_papers:
                all_papers[p["id"]] = p
    except Exception as e:
        print(f"  CrossRef fetch failed: {e}")

    # Clean HTML tags from titles/abstracts
    for p in all_papers.values():
        if p.get("title"):
            p["title"] = re.sub(r"<[^>]+>", "", p["title"]).strip()
        if p.get("abstract"):
            p["abstract"] = re.sub(r"<[^>]+>", "", p["abstract"]).strip()

    papers_list = list(all_papers.values())
    print(f"\nTotal unique papers: {len(papers_list)}")

    if not papers_list:
        print("No papers found. Done.")
        return

    # Step 2: Score relevance
    print("\n--- Scoring relevance ---")
    scores = score_papers_batch(papers_list, profile)
    print(f"Generated {len(scores)} relevance scores")

    # Step 3: Store in database
    print("\n--- Storing results ---")
    conn = models.get_connection()
    models.init_db(conn)

    paper_count = 0
    for paper in papers_list:
        conn.execute(
            """INSERT OR REPLACE INTO papers
            (id, source, title, abstract, year, publication_date, doi, journal,
             cited_by_count, type, open_access_url, fetched_at, digest_week)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                paper["id"], paper["source"], paper["title"], paper["abstract"],
                paper["year"], paper["publication_date"], paper["doi"], paper["journal"],
                paper["cited_by_count"], paper["type"], paper["open_access_url"],
                now.isoformat(), week_str,
            ),
        )
        paper_count += 1

        # Authors
        for author in paper.get("authors", []):
            conn.execute(
                "INSERT OR IGNORE INTO paper_authors (paper_id, author_name, author_id, institution, position) VALUES (?, ?, ?, ?, ?)",
                (paper["id"], author["author_name"], author["author_id"], author["institution"], author["position"]),
            )

        # Concepts
        for concept in paper.get("concepts", []):
            conn.execute(
                "INSERT OR IGNORE INTO paper_concepts (paper_id, concept_name, score) VALUES (?, ?, ?)",
                (paper["id"], concept["concept_name"], concept["score"]),
            )

    # Store relevance scores
    for paper_id, department, score_dict in scores:
        conn.execute(
            """INSERT OR REPLACE INTO paper_relevance
            (paper_id, department, relevance_score, semantic_score, concept_overlap_score, explanation)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (
                paper_id, department,
                score_dict["relevance_score"], score_dict["semantic_score"],
                score_dict["concept_overlap_score"], score_dict["explanation"],
            ),
        )

    # Create/update digest record
    conn.execute(
        """INSERT OR REPLACE INTO digests (week, generated_at, paper_count, date_from, date_to, status)
        VALUES (?, ?, ?, ?, ?, 'complete')""",
        (week_str, now.isoformat(), paper_count, date_from, date_to),
    )

    conn.commit()

    # Step 4: Enrich papers with key messages + data availability
    print("\n--- Enriching papers ---")
    from .enrichment import enrich_papers
    enrichments = enrich_papers(papers_list)
    for paper_id, enrich in enrichments:
        conn.execute("""
            INSERT OR REPLACE INTO paper_enrichment
            (paper_id, key_messages, data_available, data_url, data_repository, methods_summary, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            paper_id, enrich["key_messages"], int(enrich["data_available"]),
            enrich["data_url"], enrich["data_repository"], enrich["methods_summary"],
            now.isoformat(),
        ))
    conn.commit()
    print(f"Enriched {len(enrichments)} papers")

    # Step 5: Fetch Altmetric data for top papers
    print("\n--- Fetching Altmetric data ---")
    top_papers = sorted(papers_list, key=lambda p: len(p.get("concepts", [])), reverse=True)[:100]
    try:
        from .fetchers import altmetric
        alt_data = altmetric.fetch_for_papers(top_papers)
        for paper_id, adata in alt_data.items():
            conn.execute("""
                INSERT OR REPLACE INTO paper_altmetric
                (paper_id, altmetric_score, news_count, blog_count, twitter_count, policy_count, news_sources, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper_id, adata["altmetric_score"], adata["news_count"],
                adata["blog_count"], adata["twitter_count"], adata["policy_count"],
                adata["news_sources"], now.isoformat(),
            ))
        conn.commit()
    except Exception as e:
        print(f"  Altmetric fetch failed: {e}")

    # Step 6: Fetch Bluesky social mentions for top papers
    print("\n--- Fetching Bluesky mentions ---")
    top_headlines = sorted(papers_list, key=lambda p: len(p.get("concepts", [])), reverse=True)[:20]
    try:
        from .fetchers import bluesky
        social_data = bluesky.fetch_social_for_papers(top_headlines)
        for paper_id, posts in social_data.items():
            for post in posts:
                conn.execute("""
                    INSERT OR REPLACE INTO paper_social
                    (paper_id, platform, post_uri, author_handle, author_name, text, likes, reposts, posted_at, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    paper_id, post["platform"], post["post_uri"],
                    post["author_handle"], post["author_name"], post["text"],
                    post["likes"], post["reposts"], post["posted_at"],
                    now.isoformat(),
                ))
        conn.commit()
    except Exception as e:
        print(f"  Bluesky fetch failed: {e}")

    conn.close()

    print(f"\nDone! Stored {paper_count} papers for week {week_str}")
