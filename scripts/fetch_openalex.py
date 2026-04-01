"""Fetch all IGB-affiliated publications from OpenAlex and store in SQLite."""

import json
import os
import sqlite3
import time
import requests

BASE_URL = "https://api.openalex.org"
IGB_ID = "I4210116314"
MAILTO = "erika.freeman@igb-berlin.de"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


def reconstruct_abstract(inverted_index: dict) -> str:
    if not inverted_index:
        return ""
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS publications (
            id TEXT PRIMARY KEY,
            title TEXT,
            abstract TEXT,
            year INTEGER,
            doi TEXT,
            journal TEXT,
            cited_by_count INTEGER,
            type TEXT
        );
        CREATE TABLE IF NOT EXISTS authors (
            id TEXT PRIMARY KEY,
            display_name TEXT,
            orcid TEXT
        );
        CREATE TABLE IF NOT EXISTS publication_authors (
            publication_id TEXT,
            author_id TEXT,
            is_igb_affiliated INTEGER,
            PRIMARY KEY (publication_id, author_id),
            FOREIGN KEY (publication_id) REFERENCES publications(id),
            FOREIGN KEY (author_id) REFERENCES authors(id)
        );
        CREATE TABLE IF NOT EXISTS concepts (
            publication_id TEXT,
            concept_name TEXT,
            score REAL,
            FOREIGN KEY (publication_id) REFERENCES publications(id)
        );
        CREATE INDEX IF NOT EXISTS idx_pub_year ON publications(year);
        CREATE INDEX IF NOT EXISTS idx_pub_authors_pub ON publication_authors(publication_id);
        CREATE INDEX IF NOT EXISTS idx_pub_authors_auth ON publication_authors(author_id);
        CREATE INDEX IF NOT EXISTS idx_concepts_pub ON concepts(publication_id);
    """)


def fetch_all_works():
    """Cursor-paginate through all IGB works."""
    cursor = "*"
    all_works = []
    page = 0

    while cursor:
        url = (
            f"{BASE_URL}/works?"
            f"filter=institutions.id:{IGB_ID}"
            f"&per_page=200"
            f"&cursor={cursor}"
            f"&mailto={MAILTO}"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        all_works.extend(results)
        page += 1
        count = len(all_works)
        total = data.get("meta", {}).get("count", "?")
        print(f"  Page {page}: fetched {len(results)} works ({count}/{total} total)")

        cursor = data.get("meta", {}).get("next_cursor")
        time.sleep(0.1)  # polite rate limiting

    return all_works


def store_works(conn: sqlite3.Connection, works: list):
    pub_count = 0
    author_count = 0

    for work in works:
        work_id = work.get("id", "")
        title = work.get("title", "")
        abstract_inv = work.get("abstract_inverted_index")
        abstract = reconstruct_abstract(abstract_inv) if abstract_inv else ""
        year = work.get("publication_year")
        doi = work.get("doi", "")
        journal = ""
        primary_loc = work.get("primary_location") or {}
        source = primary_loc.get("source") or {}
        journal = source.get("display_name", "")
        cited_by = work.get("cited_by_count", 0)
        work_type = work.get("type", "")

        conn.execute(
            "INSERT OR REPLACE INTO publications VALUES (?,?,?,?,?,?,?,?)",
            (work_id, title, abstract, year, doi, journal, cited_by, work_type),
        )
        pub_count += 1

        # Authors
        for authorship in work.get("authorships", []):
            author = authorship.get("author") or {}
            author_id = author.get("id", "")
            if not author_id:
                continue
            display_name = author.get("display_name", "")
            orcid = author.get("orcid", "")

            conn.execute(
                "INSERT OR REPLACE INTO authors VALUES (?,?,?)",
                (author_id, display_name, orcid),
            )

            # Check IGB affiliation for this authorship
            institutions = authorship.get("institutions", [])
            is_igb = any(
                IGB_ID in (inst.get("id", "") or "")
                for inst in institutions
            )

            conn.execute(
                "INSERT OR REPLACE INTO publication_authors VALUES (?,?,?)",
                (work_id, author_id, int(is_igb)),
            )

        # Concepts/topics
        for concept in work.get("concepts", []):
            concept_name = concept.get("display_name", "")
            score = concept.get("score", 0.0)
            if concept_name:
                conn.execute(
                    "INSERT INTO concepts VALUES (?,?,?)",
                    (work_id, concept_name, score),
                )

    conn.commit()
    print(f"  Stored {pub_count} publications")


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    print("Fetching IGB publications from OpenAlex...")
    works = fetch_all_works()
    print(f"Fetched {len(works)} works total")

    print("Storing in database...")
    store_works(conn, works)

    # Summary stats
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM publications")
    total_pubs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM publications WHERE abstract != ''")
    with_abstract = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT author_id) FROM publication_authors WHERE is_igb_affiliated = 1")
    igb_authors = cur.fetchone()[0]

    print(f"\nDatabase summary:")
    print(f"  Publications: {total_pubs}")
    print(f"  With abstracts: {with_abstract}")
    print(f"  IGB-affiliated authors: {igb_authors}")

    conn.close()


if __name__ == "__main__":
    main()
