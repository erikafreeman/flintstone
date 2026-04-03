"""SQLite data access layer for Scopefish."""

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "scopefish.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection):
    """Create all tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS papers (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT,
            abstract TEXT,
            year INTEGER,
            publication_date TEXT,
            doi TEXT,
            journal TEXT,
            cited_by_count INTEGER DEFAULT 0,
            type TEXT,
            open_access_url TEXT,
            fetched_at TEXT,
            digest_week TEXT
        );

        CREATE TABLE IF NOT EXISTS paper_authors (
            paper_id TEXT,
            author_name TEXT,
            author_id TEXT,
            institution TEXT,
            position INTEGER,
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        );

        CREATE TABLE IF NOT EXISTS paper_concepts (
            paper_id TEXT,
            concept_name TEXT,
            score REAL,
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        );

        CREATE TABLE IF NOT EXISTS igb_topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            concept_name TEXT UNIQUE,
            paper_count INTEGER,
            avg_score REAL,
            department TEXT
        );

        CREATE TABLE IF NOT EXISTS department_profiles (
            department TEXT PRIMARY KEY,
            top_concepts TEXT,
            description TEXT,
            query_string TEXT
        );

        CREATE TABLE IF NOT EXISTS paper_relevance (
            paper_id TEXT,
            department TEXT,
            relevance_score REAL,
            semantic_score REAL,
            concept_overlap_score REAL,
            explanation TEXT,
            PRIMARY KEY (paper_id, department),
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        );

        CREATE TABLE IF NOT EXISTS tracked_researchers (
            author_id TEXT PRIMARY KEY,
            display_name TEXT,
            institution TEXT,
            reason TEXT,
            added_at TEXT
        );

        CREATE TABLE IF NOT EXISTS digests (
            week TEXT PRIMARY KEY,
            generated_at TEXT,
            paper_count INTEGER,
            date_from TEXT,
            date_to TEXT,
            status TEXT DEFAULT 'pending'
        );

        CREATE INDEX IF NOT EXISTS idx_papers_date ON papers(publication_date);
        CREATE INDEX IF NOT EXISTS idx_papers_week ON papers(digest_week);
        CREATE INDEX IF NOT EXISTS idx_paper_concepts_paper ON paper_concepts(paper_id);
        CREATE INDEX IF NOT EXISTS idx_paper_relevance_dept ON paper_relevance(department);
        CREATE INDEX IF NOT EXISTS idx_paper_relevance_score ON paper_relevance(relevance_score);
        CREATE INDEX IF NOT EXISTS idx_paper_authors_paper ON paper_authors(paper_id);

        CREATE TABLE IF NOT EXISTS custom_feeds (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            keywords TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS paper_social (
            paper_id TEXT,
            platform TEXT,
            post_uri TEXT,
            author_handle TEXT,
            author_name TEXT,
            text TEXT,
            likes INTEGER DEFAULT 0,
            reposts INTEGER DEFAULT 0,
            posted_at TEXT,
            fetched_at TEXT,
            PRIMARY KEY (paper_id, post_uri),
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        );

        CREATE TABLE IF NOT EXISTS paper_altmetric (
            paper_id TEXT PRIMARY KEY,
            altmetric_score REAL DEFAULT 0,
            news_count INTEGER DEFAULT 0,
            blog_count INTEGER DEFAULT 0,
            twitter_count INTEGER DEFAULT 0,
            policy_count INTEGER DEFAULT 0,
            news_sources TEXT,
            fetched_at TEXT,
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        );

        CREATE TABLE IF NOT EXISTS paper_enrichment (
            paper_id TEXT PRIMARY KEY,
            key_messages TEXT,
            data_available INTEGER DEFAULT 0,
            data_url TEXT,
            data_repository TEXT,
            methods_summary TEXT,
            fetched_at TEXT,
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        );

        CREATE TABLE IF NOT EXISTS funding_calls (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            funder TEXT,
            description TEXT,
            url TEXT,
            deadline TEXT,
            amount TEXT,
            keywords TEXT,
            region TEXT,
            fetched_at TEXT
        );

        CREATE TABLE IF NOT EXISTS funded_projects (
            project_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT,
            acronym TEXT,
            description TEXT,
            funder TEXT,
            programme TEXT,
            total_cost TEXT,
            start_date TEXT,
            end_date TEXT,
            status TEXT,
            coordinator TEXT,
            url TEXT,
            keywords TEXT,
            fetched_at TEXT
        );

        CREATE TABLE IF NOT EXISTS press_releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT UNIQUE,
            description TEXT,
            source TEXT,
            pub_date TEXT,
            fetched_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_paper_social_paper ON paper_social(paper_id);
        CREATE INDEX IF NOT EXISTS idx_funding_deadline ON funding_calls(deadline);
        CREATE INDEX IF NOT EXISTS idx_funded_projects_funder ON funded_projects(funder);
        CREATE INDEX IF NOT EXISTS idx_funded_projects_date ON funded_projects(start_date);
    """)


def get_stats(conn: sqlite3.Connection) -> dict:
    """Return aggregate stats for the landing page."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM papers")
    total_papers = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT concept_name) FROM igb_topics")
    topics_tracked = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM digests WHERE status = 'complete'")
    digests_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tracked_researchers")
    researchers_count = cur.fetchone()[0]

    # This week's papers
    now = datetime.now()
    week_str = now.strftime("%G-W%V")
    cur.execute("SELECT COUNT(*) FROM papers WHERE digest_week = ?", (week_str,))
    this_week = cur.fetchone()[0]

    # Sources
    cur.execute("SELECT COUNT(DISTINCT source) FROM papers")
    sources = cur.fetchone()[0]

    return {
        "total_papers": total_papers,
        "topics_tracked": topics_tracked,
        "digests_count": digests_count,
        "researchers_tracked": researchers_count,
        "this_week": this_week,
        "sources": max(sources, 3),
    }


def get_current_digest(conn: sqlite3.Connection) -> dict:
    """Get the most recent completed digest with its papers."""
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM digests WHERE status = 'complete' ORDER BY week DESC LIMIT 1"
    )
    digest = cur.fetchone()
    if not digest:
        return None

    return get_digest(conn, dict(digest)["week"])


def get_digest(conn: sqlite3.Connection, week: str) -> Optional[dict]:
    """Get a specific digest by week string."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM digests WHERE week = ?", (week,))
    row = cur.fetchone()
    if not row:
        return None
    digest = dict(row)

    # Get papers for this week, ranked by relevance (institute-wide)
    cur.execute("""
        SELECT p.*, pr.relevance_score, pr.semantic_score,
               pr.concept_overlap_score, pr.explanation
        FROM papers p
        LEFT JOIN paper_relevance pr ON p.id = pr.paper_id AND pr.department = 'all'
        WHERE p.digest_week = ?
        ORDER BY pr.relevance_score DESC NULLS LAST
    """, (week,))
    papers = [dict(r) for r in cur.fetchall()]

    # Attach concepts and authors to each paper
    for paper in papers:
        cur.execute(
            "SELECT concept_name, score FROM paper_concepts WHERE paper_id = ? ORDER BY score DESC LIMIT 5",
            (paper["id"],),
        )
        paper["concepts"] = [dict(r) for r in cur.fetchall()]

        cur.execute(
            "SELECT author_name, author_id, institution FROM paper_authors WHERE paper_id = ? ORDER BY position",
            (paper["id"],),
        )
        paper["authors"] = [dict(r) for r in cur.fetchall()]
        paper["authors_str"] = ", ".join(a["author_name"] for a in paper["authors"][:5])
        if len(paper["authors"]) > 5:
            paper["authors_str"] += f" + {len(paper['authors']) - 5} more"

        # Abstract snippet
        abstract = paper.get("abstract") or ""
        paper["abstract_short"] = abstract[:250] + ("..." if len(abstract) > 250 else "")

        # Relevance tier
        score = paper.get("relevance_score") or 0
        if score >= 0.7:
            paper["tier"] = "high"
        elif score >= 0.4:
            paper["tier"] = "medium"
        else:
            paper["tier"] = "notable"

    digest["papers"] = papers
    digest["high_count"] = sum(1 for p in papers if p.get("tier") == "high")
    digest["medium_count"] = sum(1 for p in papers if p.get("tier") == "medium")
    digest["notable_count"] = sum(1 for p in papers if p.get("tier") == "notable")

    # Available weeks for navigation
    cur.execute("SELECT week FROM digests WHERE status = 'complete' ORDER BY week DESC LIMIT 20")
    digest["available_weeks"] = [r[0] for r in cur.fetchall()]

    return digest


def get_department_feed(conn: sqlite3.Connection, department: str, limit: int = 50) -> list:
    """Get papers relevant to a specific department."""
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*, pr.relevance_score, pr.explanation
        FROM papers p
        JOIN paper_relevance pr ON p.id = pr.paper_id
        WHERE pr.department = ?
        ORDER BY pr.relevance_score DESC
        LIMIT ?
    """, (department, limit))
    papers = [dict(r) for r in cur.fetchall()]

    for paper in papers:
        cur.execute(
            "SELECT concept_name FROM paper_concepts WHERE paper_id = ? ORDER BY score DESC LIMIT 5",
            (paper["id"],),
        )
        paper["concepts"] = [r[0] for r in cur.fetchall()]

        cur.execute(
            "SELECT author_name FROM paper_authors WHERE paper_id = ? ORDER BY position LIMIT 5",
            (paper["id"],),
        )
        names = [r[0] for r in cur.fetchall()]
        paper["authors_str"] = ", ".join(names)

        abstract = paper.get("abstract") or ""
        paper["abstract_short"] = abstract[:200] + ("..." if len(abstract) > 200 else "")

    return papers


def get_paper_detail(conn: sqlite3.Connection, paper_id: str) -> Optional[dict]:
    """Get full paper details with all relevance info."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM papers WHERE id = ?", (paper_id,))
    row = cur.fetchone()
    if not row:
        return None
    paper = dict(row)

    cur.execute(
        "SELECT author_name, author_id, institution FROM paper_authors WHERE paper_id = ? ORDER BY position",
        (paper_id,),
    )
    paper["authors"] = [dict(r) for r in cur.fetchall()]

    cur.execute(
        "SELECT concept_name, score FROM paper_concepts WHERE paper_id = ? ORDER BY score DESC",
        (paper_id,),
    )
    paper["concepts"] = [dict(r) for r in cur.fetchall()]

    cur.execute(
        "SELECT department, relevance_score, semantic_score, concept_overlap_score, explanation "
        "FROM paper_relevance WHERE paper_id = ? ORDER BY relevance_score DESC",
        (paper_id,),
    )
    paper["relevance"] = [dict(r) for r in cur.fetchall()]

    return paper


def get_departments(conn: sqlite3.Connection) -> list:
    """Get all department profiles with paper counts."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM department_profiles ORDER BY department")
    depts = [dict(r) for r in cur.fetchall()]

    for dept in depts:
        cur.execute(
            "SELECT COUNT(*) FROM paper_relevance WHERE department = ? AND relevance_score >= 0.4",
            (dept["department"],),
        )
        dept["paper_count"] = cur.fetchone()[0]

    return depts


def get_tracked_researchers(conn: sqlite3.Connection) -> list:
    """Get tracked researchers with their recent paper counts."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM tracked_researchers ORDER BY display_name")
    researchers = [dict(r) for r in cur.fetchall()]

    for r in researchers:
        cur.execute(
            "SELECT COUNT(*) FROM paper_authors WHERE author_id = ?",
            (r["author_id"],),
        )
        r["paper_count"] = cur.fetchone()[0]

    return researchers


# Top-tier journals for freshwater/ecology/general science — papers here get a ranking boost
TOP_JOURNALS = {
    # General top-tier
    "nature": 1.0, "science": 1.0, "proceedings of the national academy of sciences": 0.95,
    "pnas": 0.95, "nature communications": 0.9, "science advances": 0.9,
    "cell": 0.9, "the lancet": 0.85, "new england journal of medicine": 0.85,
    # Ecology & environment top-tier
    "nature ecology & evolution": 0.95, "nature ecology and evolution": 0.95,
    "nature climate change": 0.9, "nature geoscience": 0.9, "nature water": 0.9,
    "nature sustainability": 0.9, "nature food": 0.85,
    "ecology letters": 0.85, "trends in ecology & evolution": 0.85,
    "trends in ecology and evolution": 0.85, "global change biology": 0.85,
    "annual review of ecology, evolution, and systematics": 0.85,
    # Ecology & freshwater journals
    "limnology and oceanography": 0.8, "freshwater biology": 0.8,
    "water research": 0.8, "environmental science & technology": 0.8,
    "environmental science and technology": 0.8,
    "global ecology and biogeography": 0.8, "journal of ecology": 0.8,
    "functional ecology": 0.75, "journal of animal ecology": 0.75,
    "molecular ecology": 0.75, "ecography": 0.75,
    "ecological monographs": 0.8, "ecology": 0.75,
    "oikos": 0.7, "oecologia": 0.7, "ecosystems": 0.75,
    "biogeosciences": 0.7, "bioscience": 0.75,
    "conservation biology": 0.75, "biological conservation": 0.7,
    "fish and fisheries": 0.75, "journal of applied ecology": 0.75,
    "aquatic sciences": 0.7, "hydrobiologia": 0.65,
    "inland waters": 0.7, "journal of fish biology": 0.65,
    "environmental pollution": 0.7, "science of the total environment": 0.65,
    "environmental research letters": 0.75,
    "proceedings of the royal society b": 0.8,
    "current biology": 0.85, "elife": 0.85,
    "isme journal": 0.8, "the isme journal": 0.8,
    "microbiome": 0.75, "environmental microbiology": 0.75,
}


def _journal_boost(journal_name: str) -> float:
    """Return a 0-1 quality boost for a journal. 0 = unknown, 1 = Nature/Science."""
    if not journal_name:
        return 0.0
    key = journal_name.lower().strip()
    return TOP_JOURNALS.get(key, 0.0)


def get_custom_feeds(conn: sqlite3.Connection) -> list:
    """Get all custom feeds."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM custom_feeds ORDER BY updated_at DESC")
    feeds = [dict(r) for r in cur.fetchall()]
    for feed in feeds:
        keywords = [k.strip() for k in (feed.get("keywords") or "").split(",") if k.strip()]
        feed["keyword_list"] = keywords
        feed["keyword_count"] = len(keywords)
    return feeds


def get_custom_feed(conn: sqlite3.Connection, feed_id: str) -> Optional[dict]:
    """Get a single custom feed by ID."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM custom_feeds WHERE id = ?", (feed_id,))
    row = cur.fetchone()
    if not row:
        return None
    feed = dict(row)
    feed["keyword_list"] = [k.strip() for k in (feed.get("keywords") or "").split(",") if k.strip()]
    return feed


def save_custom_feed(conn: sqlite3.Connection, feed_id: str, name: str, description: str, keywords: str):
    """Create or update a custom feed."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO custom_feeds (id, name, description, keywords, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            description = excluded.description,
            keywords = excluded.keywords,
            updated_at = excluded.updated_at
    """, (feed_id, name, description, keywords, now, now))
    conn.commit()


def delete_custom_feed(conn: sqlite3.Connection, feed_id: str):
    """Delete a custom feed."""
    conn.execute("DELETE FROM custom_feeds WHERE id = ?", (feed_id,))
    conn.commit()


def get_custom_feed_papers(conn: sqlite3.Connection, keywords: list, limit: int = 50) -> list:
    """Get papers matching custom feed keywords, ranked by match quality + journal tier."""
    if not keywords:
        return []

    # First: find candidate papers via simple OR filter (fast)
    or_conditions = []
    filter_params = []
    for kw in keywords[:20]:
        or_conditions.append("(p.title LIKE ? OR p.abstract LIKE ?)")
        filter_params.extend([f"%{kw}%", f"%{kw}%"])

    # Also include papers whose concepts match
    concept_or = []
    concept_params = []
    for kw in keywords[:20]:
        concept_or.append("pc2.concept_name LIKE ?")
        concept_params.append(f"%{kw}%")

    text_where = " OR ".join(or_conditions)
    concept_where = " OR ".join(concept_or)

    cur = conn.execute(f"""
        SELECT DISTINCT p.*, pr.relevance_score as igb_relevance
        FROM papers p
        LEFT JOIN paper_relevance pr ON p.id = pr.paper_id AND pr.department = 'all'
        WHERE ({text_where})
           OR p.id IN (SELECT paper_id FROM paper_concepts pc2 WHERE {concept_where})
        ORDER BY pr.relevance_score DESC NULLS LAST
        LIMIT ?
    """, filter_params + concept_params + [limit * 2])

    papers = [dict(r) for r in cur.fetchall()]

    kw_lower = [kw.lower() for kw in keywords]

    for paper in papers:
        # Count keyword matches in title + abstract
        title_lower = (paper.get("title") or "").lower()
        abstract_lower = (paper.get("abstract") or "").lower()
        text_matches = sum(1 for kw in kw_lower if kw in title_lower or kw in abstract_lower)

        # Count concept matches
        cur2 = conn.execute(
            "SELECT concept_name FROM paper_concepts WHERE paper_id = ?", (paper["id"],)
        )
        paper_concepts_lower = [r[0].lower() for r in cur2.fetchall()]
        concept_matches = sum(1 for kw in kw_lower if any(kw in c for c in paper_concepts_lower))

        total_keywords = len(keywords)
        keyword_score = min((text_matches + concept_matches * 0.5) / max(total_keywords * 0.4, 1), 1.0)

        # Journal quality boost: top journals get significant ranking lift
        journal = paper.get("journal") or ""
        j_boost = _journal_boost(journal)
        paper["journal_tier"] = (
            "top" if j_boost >= 0.9 else
            "high" if j_boost >= 0.75 else
            "good" if j_boost >= 0.6 else
            None
        )

        # Final score: 60% keyword match + 30% journal quality + 10% citation signal
        citation_signal = min((paper.get("cited_by_count") or 0) / 50.0, 1.0)
        paper["custom_score"] = min(
            0.6 * keyword_score + 0.3 * j_boost + 0.1 * citation_signal,
            1.0
        )

        # Find which keywords matched
        paper["matched_keywords"] = [
            kw for kw in keywords
            if kw.lower() in title_lower or kw.lower() in abstract_lower
        ]

        cur2 = conn.execute(
            "SELECT concept_name FROM paper_concepts WHERE paper_id = ? ORDER BY score DESC LIMIT 5",
            (paper["id"],),
        )
        paper["concepts"] = [r[0] for r in cur2.fetchall()]

        cur2 = conn.execute(
            "SELECT author_name FROM paper_authors WHERE paper_id = ? ORDER BY position LIMIT 5",
            (paper["id"],),
        )
        paper["authors_str"] = ", ".join(r[0] for r in cur2.fetchall())

        abstract = paper.get("abstract") or ""
        paper["abstract_short"] = abstract[:200] + ("..." if len(abstract) > 200 else "")

    # Re-sort by the combined score (journal boost changes ranking order)
    papers.sort(key=lambda p: p.get("custom_score", 0), reverse=True)

    return papers


def search_papers(conn: sqlite3.Connection, query: str, limit: int = 30) -> list:
    """Simple keyword search across papers."""
    import re
    words = re.findall(r'[a-zA-Z0-9][\w-]{2,}', query)
    if not words:
        return []

    conditions = []
    params = []
    for word in words[:10]:
        conditions.append("(p.title LIKE ? OR p.abstract LIKE ?)")
        params.extend([f"%{word}%", f"%{word}%"])

    where = " OR ".join(conditions)
    cur = conn.execute(f"""
        SELECT p.*, pr.relevance_score, pr.explanation
        FROM papers p
        LEFT JOIN paper_relevance pr ON p.id = pr.paper_id AND pr.department = 'all'
        WHERE {where}
        ORDER BY pr.relevance_score DESC NULLS LAST
        LIMIT ?
    """, params + [limit])

    papers = [dict(r) for r in cur.fetchall()]
    for paper in papers:
        cur2 = conn.execute(
            "SELECT concept_name FROM paper_concepts WHERE paper_id = ? ORDER BY score DESC LIMIT 5",
            (paper["id"],),
        )
        paper["concepts"] = [r[0] for r in cur2.fetchall()]

        cur2 = conn.execute(
            "SELECT author_name FROM paper_authors WHERE paper_id = ? ORDER BY position LIMIT 5",
            (paper["id"],),
        )
        paper["authors_str"] = ", ".join(r[0] for r in cur2.fetchall())

        abstract_s = paper.get("abstract") or ""
        paper["abstract_short"] = abstract_s[:200] + ("..." if len(abstract_s) > 200 else "")

    return papers


def get_headlines(conn: sqlite3.Connection, limit: int = 10) -> list:
    """Get top papers for the headlines section.

    Ranked by: journal quality + relevance + social attention + citations.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*, pr.relevance_score, pr.explanation,
               COALESCE(pa.altmetric_score, 0) as altmetric_score,
               COALESCE(pa.news_count, 0) as news_count,
               COALESCE(pa.twitter_count, 0) as twitter_count
        FROM papers p
        LEFT JOIN paper_relevance pr ON p.id = pr.paper_id AND pr.department = 'all'
        LEFT JOIN paper_altmetric pa ON p.id = pa.paper_id
        ORDER BY
            COALESCE(pa.altmetric_score, 0) * 0.3 +
            COALESCE(pr.relevance_score, 0) * 0.4 +
            MIN(COALESCE(p.cited_by_count, 0) / 20.0, 1.0) * 0.3
            DESC
        LIMIT ?
    """, (limit * 3,))
    papers = [dict(r) for r in cur.fetchall()]

    for paper in papers:
        j_boost = _journal_boost(paper.get("journal") or "")
        paper["journal_tier"] = (
            "top" if j_boost >= 0.9 else
            "high" if j_boost >= 0.75 else
            "good" if j_boost >= 0.6 else
            None
        )
        paper["headline_score"] = (
            0.3 * j_boost +
            0.3 * (paper.get("relevance_score") or 0) +
            0.2 * min((paper.get("altmetric_score") or 0) / 50.0, 1.0) +
            0.2 * min((paper.get("cited_by_count") or 0) / 20.0, 1.0)
        )

        cur2 = conn.execute(
            "SELECT concept_name FROM paper_concepts WHERE paper_id = ? ORDER BY score DESC LIMIT 5",
            (paper["id"],),
        )
        paper["concepts"] = [r[0] for r in cur2.fetchall()]

        cur2 = conn.execute(
            "SELECT author_name FROM paper_authors WHERE paper_id = ? ORDER BY position LIMIT 5",
            (paper["id"],),
        )
        paper["authors_str"] = ", ".join(r[0] for r in cur2.fetchall())

        abstract = paper.get("abstract") or ""
        paper["abstract_short"] = abstract[:200] + ("..." if len(abstract) > 200 else "")

        cur2 = conn.execute(
            "SELECT COUNT(*) FROM paper_social WHERE paper_id = ?", (paper["id"],)
        )
        paper["social_count"] = cur2.fetchone()[0]

        cur2 = conn.execute(
            "SELECT key_messages, data_available, data_url, data_repository FROM paper_enrichment WHERE paper_id = ?",
            (paper["id"],),
        )
        enrich = cur2.fetchone()
        if enrich:
            paper["key_messages"] = enrich[0]
            paper["data_available"] = bool(enrich[1])
            paper["data_url"] = enrich[2]
            paper["data_repository"] = enrich[3]

    papers.sort(key=lambda p: p.get("headline_score", 0), reverse=True)
    return papers[:limit]


def get_paper_social(conn: sqlite3.Connection, paper_id: str) -> list:
    """Get social media posts about a paper."""
    cur = conn.execute("""
        SELECT * FROM paper_social WHERE paper_id = ?
        ORDER BY likes DESC, posted_at DESC
    """, (paper_id,))
    return [dict(r) for r in cur.fetchall()]


def get_paper_altmetric(conn: sqlite3.Connection, paper_id: str) -> Optional[dict]:
    """Get Altmetric data for a paper."""
    cur = conn.execute("SELECT * FROM paper_altmetric WHERE paper_id = ?", (paper_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_paper_enrichment(conn: sqlite3.Connection, paper_id: str) -> Optional[dict]:
    """Get enrichment data for a paper."""
    cur = conn.execute("SELECT * FROM paper_enrichment WHERE paper_id = ?", (paper_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_funding_calls(conn: sqlite3.Connection, active_only: bool = True) -> list:
    """Get funding calls, optionally filtered to active ones."""
    if active_only:
        cur = conn.execute("""
            SELECT * FROM funding_calls
            WHERE deadline >= date('now') OR deadline IS NULL OR deadline = ''
            ORDER BY deadline ASC NULLS LAST
        """)
    else:
        cur = conn.execute("SELECT * FROM funding_calls ORDER BY deadline ASC NULLS LAST")
    all_calls = [dict(r) for r in cur.fetchall()]
    # Deduplicate by title (SEDIA API returns sub-lots with same title)
    seen_titles = set()
    calls = []
    for call in all_calls:
        title_key = (call.get("title") or "")[:60].lower()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        kw = call.get("keywords") or ""
        call["keyword_list"] = [k.strip() for k in kw.split(",") if k.strip()]
        calls.append(call)
    return calls


def save_funding_call(conn: sqlite3.Connection, call: dict):
    """Save a funding call."""
    conn.execute("""
        INSERT OR REPLACE INTO funding_calls
        (id, title, funder, description, url, deadline, amount, keywords, region, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        call["id"], call["title"], call.get("funder", ""),
        call.get("description", ""), call.get("url", ""),
        call.get("deadline", ""), call.get("amount", ""),
        call.get("keywords", ""), call.get("region", ""),
        datetime.now().isoformat(),
    ))
    conn.commit()


# ── Funded Projects ─────────────────────────────────────────────────────

def save_funded_project(conn: sqlite3.Connection, project: dict):
    """Save a funded project."""
    conn.execute("""
        INSERT OR REPLACE INTO funded_projects
        (project_id, source, title, acronym, description, funder, programme,
         total_cost, start_date, end_date, status, coordinator, url, keywords, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        project["project_id"], project["source"], project.get("title", ""),
        project.get("acronym", ""), project.get("description", ""),
        project.get("funder", ""), project.get("programme", ""),
        project.get("total_cost", ""), project.get("start_date", ""),
        project.get("end_date", ""), project.get("status", ""),
        project.get("coordinator", ""), project.get("url", ""),
        project.get("keywords", ""), datetime.now().isoformat(),
    ))


def save_funded_projects(conn: sqlite3.Connection, projects: list):
    """Save multiple funded projects."""
    for p in projects:
        save_funded_project(conn, p)
    conn.commit()


def get_funded_projects(conn: sqlite3.Connection, source: str = None,
                         funder: str = None, limit: int = 100) -> list:
    """Get funded projects with optional filters."""
    query = "SELECT * FROM funded_projects WHERE 1=1"
    params = []
    if source:
        query += " AND source = ?"
        params.append(source)
    if funder:
        query += " AND funder = ?"
        params.append(funder)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    cur = conn.execute(query, params)
    projects = [dict(r) for r in cur.fetchall()]
    for p in projects:
        # Format cost for display
        cost = p.get("total_cost", "")
        if cost and cost.isdigit():
            amt = int(cost)
            if amt >= 1_000_000:
                p["cost_display"] = f"\u20ac{amt / 1_000_000:.1f}M"
            elif amt >= 1_000:
                p["cost_display"] = f"\u20ac{amt / 1_000:.0f}K"
            else:
                p["cost_display"] = f"\u20ac{amt}"
        else:
            p["cost_display"] = cost
    return projects


def get_project_stats(conn: sqlite3.Connection) -> dict:
    """Get aggregate stats about funded projects."""
    stats = {}
    cur = conn.execute("SELECT COUNT(*) FROM funded_projects")
    stats["total"] = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(DISTINCT funder) FROM funded_projects")
    stats["funders"] = cur.fetchone()[0]
    cur = conn.execute("SELECT source, COUNT(*) as cnt FROM funded_projects GROUP BY source")
    stats["by_source"] = {row["source"]: row["cnt"] for row in cur.fetchall()}
    cur = conn.execute("SELECT funder, COUNT(*) as cnt FROM funded_projects GROUP BY funder ORDER BY cnt DESC")
    stats["by_funder"] = [(row["funder"], row["cnt"]) for row in cur.fetchall()]
    return stats


# ── Press Releases ──────────────────────────────────────────────────────

def save_press_releases(conn: sqlite3.Connection, releases: list):
    """Save press releases (skip duplicates by URL)."""
    for r in releases:
        conn.execute("""
            INSERT OR IGNORE INTO press_releases
            (title, url, description, source, pub_date, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            r.get("title", ""), r.get("url", ""),
            r.get("description", ""), r.get("source", ""),
            r.get("pub_date", ""), datetime.now().isoformat(),
        ))
    conn.commit()


def get_press_releases(conn: sqlite3.Connection, limit: int = 30) -> list:
    """Get recent press releases."""
    cur = conn.execute("""
        SELECT * FROM press_releases
        ORDER BY fetched_at DESC, pub_date DESC
        LIMIT ?
    """, (limit,))
    return [dict(r) for r in cur.fetchall()]
