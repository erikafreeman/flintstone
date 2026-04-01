"""SQLite data access layer."""

import os
import sqlite3
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_stats(conn: sqlite3.Connection) -> dict:
    """Return aggregate stats for the homepage."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM publications")
    total_pubs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM publications WHERE abstract != '' AND abstract IS NOT NULL")
    with_abstract = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT author_id) FROM publication_authors WHERE is_igb_affiliated = 1")
    igb_authors = cur.fetchone()[0]
    cur.execute("SELECT MIN(year), MAX(year) FROM publications WHERE year IS NOT NULL")
    yr = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM authors WHERE is_current_staff = 1")
    current_staff = cur.fetchone()[0]
    return {
        "total_pubs": total_pubs,
        "with_abstract": with_abstract,
        "igb_authors": igb_authors,
        "year_min": yr[0] if yr else None,
        "year_max": yr[1] if yr else None,
        "current_staff": current_staff,
    }


def fulltext_search(conn: sqlite3.Connection, query: str, limit: int = 10) -> list:
    """Full-text keyword search using FTS5. Returns publications matching exact phrases."""
    # Escape special FTS5 characters and wrap in quotes for phrase matching
    # Split on common delimiters, search each significant phrase
    import re
    words = re.findall(r'[a-zA-Z0-9][\w-]{2,}', query)
    if not words:
        return []

    # Build FTS query: search for each significant word
    fts_query = " OR ".join(f'"{w}"' for w in words[:10])

    try:
        cur = conn.execute(
            """
            SELECT p.* FROM publications_fts fts
            JOIN publications p ON fts.id = p.id
            WHERE publications_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (fts_query, limit),
        )
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_publication(conn: sqlite3.Connection, pub_id: str) -> Optional[dict]:
    cur = conn.execute("SELECT * FROM publications WHERE id = ?", (pub_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_publications_by_ids(conn: sqlite3.Connection, pub_ids: list) -> list:
    if not pub_ids:
        return []
    placeholders = ",".join("?" for _ in pub_ids)
    cur = conn.execute(
        f"SELECT * FROM publications WHERE id IN ({placeholders})", pub_ids
    )
    rows = {row["id"]: dict(row) for row in cur.fetchall()}
    return [rows[pid] for pid in pub_ids if pid in rows]


def get_authors_for_publication(conn: sqlite3.Connection, pub_id: str) -> list:
    cur = conn.execute(
        """
        SELECT a.id, a.display_name, a.orcid, pa.is_igb_affiliated
        FROM authors a
        JOIN publication_authors pa ON a.id = pa.author_id
        WHERE pa.publication_id = ?
        """,
        (pub_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def get_igb_authors_for_publications(conn: sqlite3.Connection, pub_ids: list) -> dict:
    """Return {pub_id: [author_dicts]} for IGB-affiliated authors."""
    if not pub_ids:
        return {}
    placeholders = ",".join("?" for _ in pub_ids)
    try:
        cur = conn.execute(
            f"""
            SELECT pa.publication_id, a.id, a.display_name, a.orcid,
                   a.department, a.is_current_staff
            FROM authors a
            JOIN publication_authors pa ON a.id = pa.author_id
            WHERE pa.publication_id IN ({placeholders})
              AND pa.is_igb_affiliated = 1
            """,
            pub_ids,
        )
    except Exception:
        cur = conn.execute(
            f"""
            SELECT pa.publication_id, a.id, a.display_name, a.orcid
            FROM authors a
            JOIN publication_authors pa ON a.id = pa.author_id
            WHERE pa.publication_id IN ({placeholders})
              AND pa.is_igb_affiliated = 1
            """,
            pub_ids,
        )
    result = {}
    for row in cur.fetchall():
        pid = row["publication_id"]
        if pid not in result:
            result[pid] = []
        result[pid].append(dict(row))
    return result


def get_all_authors_for_publications(conn: sqlite3.Connection, pub_ids: list) -> dict:
    """Return {pub_id: [author_dicts]} for all authors, including department info."""
    if not pub_ids:
        return {}
    placeholders = ",".join("?" for _ in pub_ids)
    cur = conn.execute(
        f"""
        SELECT pa.publication_id, a.id, a.display_name, a.orcid, pa.is_igb_affiliated,
               a.department, a.is_current_staff
        FROM authors a
        JOIN publication_authors pa ON a.id = pa.author_id
        WHERE pa.publication_id IN ({placeholders})
        """,
        pub_ids,
    )
    result = {}
    for row in cur.fetchall():
        pid = row["publication_id"]
        if pid not in result:
            result[pid] = []
        result[pid].append(dict(row))
    return result


def get_authors_in_department(conn: sqlite3.Connection, department: str) -> list:
    """Return all authors in a given department."""
    cur = conn.execute(
        "SELECT id, display_name FROM authors WHERE department = ? AND is_current_staff = 1",
        (department,),
    )
    return [dict(row) for row in cur.fetchall()]


def get_author(conn: sqlite3.Connection, author_id: str) -> Optional[dict]:
    try:
        cur = conn.execute("SELECT id, display_name, orcid, department, position, is_current_staff FROM authors WHERE id = ?", (author_id,))
    except sqlite3.OperationalError:
        cur = conn.execute("SELECT id, display_name, orcid FROM authors WHERE id = ?", (author_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_publications_by_author(conn: sqlite3.Connection, author_id: str, limit: int = 50) -> list:
    """Return publications by a specific author, most recent first."""
    cur = conn.execute(
        """
        SELECT p.*, pa.is_igb_affiliated
        FROM publications p
        JOIN publication_authors pa ON p.id = pa.publication_id
        WHERE pa.author_id = ?
          AND p.type != 'peer-review'
          AND p.title NOT LIKE 'Reply on RC%'
          AND p.title NOT LIKE 'Comment on %'
        ORDER BY p.year DESC, p.cited_by_count DESC
        LIMIT ?
        """,
        (author_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def get_coauthors(conn: sqlite3.Connection, author_id: str, limit: int = 10) -> list:
    """Return most frequent IGB co-authors for a given author."""
    cur = conn.execute(
        """
        SELECT a.id, a.display_name, a.orcid, COUNT(*) as collab_count
        FROM publication_authors pa1
        JOIN publication_authors pa2 ON pa1.publication_id = pa2.publication_id
        JOIN authors a ON pa2.author_id = a.id
        WHERE pa1.author_id = ?
          AND pa2.author_id != ?
          AND pa2.is_igb_affiliated = 1
        GROUP BY a.id
        ORDER BY collab_count DESC
        LIMIT ?
        """,
        (author_id, author_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def get_external_data(conn):
    """Get external collaboration data for the /external page."""
    cur = conn.cursor()

    # Check if external_authors table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='external_authors'")
    has_table = cur.fetchone() is not None

    if not has_table:
        return {"total_partners": 0, "institutions": 0, "countries": 0}, [], [], []

    # Stats
    cur.execute("SELECT COUNT(*) FROM external_authors")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT institution_name) FROM external_authors WHERE institution_name != ''")
    inst_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT institution_country) FROM external_authors WHERE institution_country != ''")
    country_count = cur.fetchone()[0]

    stats = {"total_partners": total, "institutions": inst_count, "countries": country_count}

    # Top institutions (aggregated)
    cur.execute("""
        SELECT ea.institution_name, ea.institution_country, COUNT(*) as author_count,
               GROUP_CONCAT(DISTINCT ea.top_concepts) as all_concepts
        FROM external_authors ea
        WHERE ea.institution_name != ''
        GROUP BY ea.institution_name
        ORDER BY author_count DESC
        LIMIT 30
    """)
    top_institutions = []
    for row in cur.fetchall():
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT COUNT(DISTINCT pa.publication_id)
            FROM publication_authors pa
            JOIN external_authors ea ON pa.author_id = ea.id
            WHERE ea.institution_name = ? AND pa.is_igb_affiliated = 0
        """, (row[0],))
        paper_count = cur2.fetchone()[0]

        concepts = row[3] or ""
        concept_list = list(dict.fromkeys(c.strip() for c in concepts.split(",") if c.strip()))[:5]

        top_institutions.append({
            "name": row[0],
            "country": row[1] or "?",
            "author_count": row[2],
            "paper_count": paper_count,
            "concepts": ", ".join(concept_list),
        })

    # Top individual collaborators
    cur.execute("""
        SELECT ea.id, ea.display_name, ea.orcid, ea.institution_name,
               ea.institution_country, ea.top_concepts,
               (SELECT COUNT(DISTINCT pa.publication_id)
                FROM publication_authors pa
                WHERE pa.author_id = ea.id AND pa.is_igb_affiliated = 0) as shared_papers
        FROM external_authors ea
        WHERE ea.institution_name != ''
        ORDER BY shared_papers DESC
        LIMIT 100
    """)
    collaborators = []
    for row in cur.fetchall():
        concepts = row[5] or ""
        concept_list = [c.strip() for c in concepts.split(",") if c.strip()][:4]
        collaborators.append({
            "id": row[0], "name": row[1], "orcid": row[2] or "",
            "institution": row[3], "country": row[4] or "?",
            "shared_papers": row[6], "concepts": ", ".join(concept_list),
        })

    # Institution coordinates from OpenAlex geo data (cached)
    import json as _json
    _inst_geo_path = os.path.join(os.path.dirname(__file__), "..", "data", "institution_coords.json")
    try:
        _inst_geo = _json.load(open(_inst_geo_path, encoding="utf-8"))
    except Exception:
        _inst_geo = {}

    cur.execute("""
        SELECT institution_id, institution_name, institution_country, COUNT(*) as cnt
        FROM external_authors
        WHERE institution_id != '' AND institution_name != ''
        GROUP BY institution_id
        ORDER BY cnt DESC
    """)
    inst_coords = []
    for inst_id, inst_name, country, count in cur.fetchall():
        geo = _inst_geo.get(inst_id)
        if geo and geo.get("lat") and geo.get("lon"):
            inst_coords.append({
                "lat": geo["lat"], "lon": geo["lon"],
                "name": inst_name, "country": country or "?", "count": count,
            })

    return stats, top_institutions, collaborators, inst_coords
