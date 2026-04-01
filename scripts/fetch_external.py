"""Fetch institution data for external collaborators from OpenAlex."""

import os
import sqlite3
import sys
import time
import requests

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")
BASE_URL = "https://api.openalex.org"
MAILTO = "erika.freeman@igb-berlin.de"
IGB_ID = "I4210116314"


def init_schema(conn):
    """Add institution columns if they don't exist."""
    for col in [
        "institution_name TEXT",
        "institution_country TEXT",
    ]:
        try:
            conn.execute(f"ALTER TABLE publication_authors ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    # Create external_authors summary table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS external_authors (
            id TEXT PRIMARY KEY,
            display_name TEXT,
            orcid TEXT,
            institution_name TEXT,
            institution_country TEXT,
            institution_id TEXT,
            works_count INTEGER,
            cited_by_count INTEGER,
            top_concepts TEXT
        )
    """)
    conn.commit()


def fetch_top_external_authors(conn, min_papers=3):
    """Find external authors with enough shared papers to be worth fetching."""
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.display_name, COUNT(DISTINCT pa.publication_id) as pub_count
        FROM authors a
        JOIN publication_authors pa ON a.id = pa.author_id
        WHERE pa.is_igb_affiliated = 0
        GROUP BY a.id
        HAVING pub_count >= ?
        ORDER BY pub_count DESC
    """, (min_papers,))
    return cur.fetchall()


def fetch_author_details(author_id: str) -> dict:
    """Fetch author details from OpenAlex API."""
    short_id = author_id.split("/")[-1] if "openalex.org/" in author_id else author_id
    url = f"{BASE_URL}/authors/{short_id}?mailto={MAILTO}"

    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if resp.status_code != 200:
                return {}
            return resp.json()
        except Exception:
            time.sleep(1)
    return {}


def main():
    conn = sqlite3.connect(DB_PATH)
    init_schema(conn)

    # Get external authors with 3+ shared papers
    externals = fetch_top_external_authors(conn, min_papers=3)
    print(f"Found {len(externals)} external authors with 3+ shared papers")

    # Also update institution info from existing publication data
    # (faster than API calls — use what we already fetched)
    print("\nStep 1: Extract institution data from stored publications...")
    cur = conn.cursor()

    # Re-parse institution data from the works we already have
    # We need to re-fetch works to get institution data per authorship
    # But that's expensive. Instead, let's fetch author profiles for top externals.

    print(f"\nStep 2: Fetching author profiles for top {min(len(externals), 500)} external collaborators...")

    fetched = 0
    for author_id, name, pub_count in externals[:500]:
        data = fetch_author_details(author_id)
        if not data:
            continue

        # Extract institution
        last_inst = data.get("last_known_institutions", [])
        if not last_inst:
            last_inst = data.get("last_known_institution")
            if last_inst:
                last_inst = [last_inst]
            else:
                last_inst = []

        inst_name = ""
        inst_country = ""
        inst_id = ""
        if last_inst and len(last_inst) > 0:
            inst = last_inst[0] if isinstance(last_inst, list) else last_inst
            inst_name = inst.get("display_name", "")
            inst_country = inst.get("country_code", "")
            inst_id = inst.get("id", "")

        # Extract top concepts/topics
        topics = data.get("topics", []) or data.get("x_concepts", []) or []
        top_concepts = ", ".join(t.get("display_name", "") for t in topics[:5])

        works_count = data.get("works_count", 0)
        cited_by = data.get("cited_by_count", 0)
        orcid = data.get("orcid", "")

        cur.execute("""
            INSERT OR REPLACE INTO external_authors
            (id, display_name, orcid, institution_name, institution_country,
             institution_id, works_count, cited_by_count, top_concepts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (author_id, name, orcid, inst_name, inst_country,
              inst_id, works_count, cited_by, top_concepts))

        fetched += 1
        if fetched % 50 == 0:
            conn.commit()
            print(f"  {fetched}/{min(len(externals), 500)} authors fetched")

        time.sleep(0.15)  # Polite rate limiting

    conn.commit()

    # Summary
    cur.execute("SELECT COUNT(*) FROM external_authors WHERE institution_name != ''")
    with_inst = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT institution_name) FROM external_authors WHERE institution_name != ''")
    unique_inst = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT institution_country) FROM external_authors WHERE institution_country != ''")
    unique_countries = cur.fetchone()[0]

    print(f"\n=== External Collaborator Summary ===")
    print(f"  Authors fetched: {fetched}")
    print(f"  With institution: {with_inst}")
    print(f"  Unique institutions: {unique_inst}")
    print(f"  Countries: {unique_countries}")

    # Show top institutions
    cur.execute("""
        SELECT ea.institution_name, ea.institution_country, COUNT(*) as author_count,
               SUM(
                   (SELECT COUNT(*) FROM publication_authors pa
                    WHERE pa.author_id = ea.id AND pa.is_igb_affiliated = 0)
               ) as total_papers
        FROM external_authors ea
        WHERE ea.institution_name != ''
        GROUP BY ea.institution_name
        ORDER BY author_count DESC
        LIMIT 15
    """)
    print(f"\nTop partner institutions:")
    for inst, country, authors, papers in cur.fetchall():
        print(f"  {inst} ({country}): {authors} researchers, {papers} shared papers")

    conn.close()


if __name__ == "__main__":
    main()
