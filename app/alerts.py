"""Author & Topic Alerts — follow researchers and topics for new paper notifications."""

import os
import sqlite3
import time
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")
BASE_URL = "https://api.openalex.org"
MAILTO = "erika.freeman@igb-berlin.de"


def _ensure_tables():
    """Create alerts tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,  -- 'author' or 'topic'
            name TEXT NOT NULL,
            value TEXT NOT NULL,  -- OpenAlex author ID or topic/concept string
            created_at TEXT DEFAULT (datetime('now')),
            last_checked TEXT,
            UNIQUE(type, value)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL,
            paper_id TEXT NOT NULL,
            title TEXT NOT NULL,
            authors_str TEXT DEFAULT '',
            year INTEGER,
            journal TEXT DEFAULT '',
            doi TEXT DEFAULT '',
            discovered_at TEXT DEFAULT (datetime('now')),
            is_read INTEGER DEFAULT 0,
            FOREIGN KEY (alert_id) REFERENCES alerts(id),
            UNIQUE(alert_id, paper_id)
        )
    """)
    conn.commit()
    conn.close()


def get_alerts() -> list:
    """Get all configured alerts with unread counts."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT a.*,
               COUNT(ar.id) as total_results,
               SUM(CASE WHEN ar.is_read = 0 THEN 1 ELSE 0 END) as unread_count
        FROM alerts a
        LEFT JOIN alert_results ar ON a.id = ar.alert_id
        GROUP BY a.id
        ORDER BY a.created_at DESC
    """).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def add_alert(alert_type: str, name: str, value: str) -> dict:
    """Add a new alert. Returns the alert dict or error."""
    _ensure_tables()
    if alert_type not in ("author", "topic"):
        return {"error": "Type must be 'author' or 'topic'"}

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO alerts (type, name, value) VALUES (?, ?, ?)",
            (alert_type, name, value)
        )
        conn.commit()
        alert_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()

        # Immediately check for recent papers
        check_alert(alert_id)

        return {"id": alert_id, "type": alert_type, "name": name, "value": value}
    except sqlite3.IntegrityError:
        conn.close()
        return {"error": "This alert already exists"}


def remove_alert(alert_id: int) -> bool:
    """Remove an alert and its results."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM alert_results WHERE alert_id = ?", (alert_id,))
    cur = conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
    conn.commit()
    removed = cur.rowcount > 0
    conn.close()
    return removed


def get_alert_results(alert_id: int, unread_only: bool = False) -> list:
    """Get papers found by an alert."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    query = "SELECT * FROM alert_results WHERE alert_id = ?"
    if unread_only:
        query += " AND is_read = 0"
    query += " ORDER BY discovered_at DESC LIMIT 100"
    rows = conn.execute(query, (alert_id,)).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def mark_read(alert_id: int, paper_id: str = None):
    """Mark alert results as read. If paper_id is None, mark all."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    if paper_id:
        conn.execute(
            "UPDATE alert_results SET is_read = 1 WHERE alert_id = ? AND paper_id = ?",
            (alert_id, paper_id)
        )
    else:
        conn.execute(
            "UPDATE alert_results SET is_read = 1 WHERE alert_id = ?",
            (alert_id,)
        )
    conn.commit()
    conn.close()


def check_alert(alert_id: int) -> int:
    """Check an alert for new papers. Returns count of new papers found."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    alert = conn.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    if not alert:
        conn.close()
        return 0

    alert = dict(alert)
    new_count = 0

    # Set date range — last 30 days
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    if alert["type"] == "author":
        new_count = _check_author_alert(alert, from_date)
    elif alert["type"] == "topic":
        new_count = _check_topic_alert(alert, from_date)

    # Update last_checked
    conn2 = sqlite3.connect(DB_PATH)
    conn2.execute(
        "UPDATE alerts SET last_checked = datetime('now') WHERE id = ?",
        (alert_id,)
    )
    conn2.commit()
    conn2.close()
    conn.close()

    return new_count


def _check_author_alert(alert: dict, from_date: str) -> int:
    """Check for new papers by a specific author."""
    author_id = alert["value"]
    short_id = author_id.split("/")[-1] if "openalex.org/" in author_id else author_id

    url = (
        f"{BASE_URL}/works?"
        f"filter=authorships.author.id:{short_id},"
        f"from_publication_date:{from_date}"
        f"&select=id,title,authorships,publication_year,primary_location,doi"
        f"&sort=publication_year:desc"
        f"&per_page=25"
        f"&mailto={MAILTO}"
    )

    return _fetch_and_store(alert["id"], url)


def _check_topic_alert(alert: dict, from_date: str) -> int:
    """Check for new papers matching a topic."""
    topic = alert["value"]

    url = (
        f"{BASE_URL}/works?"
        f"filter=default.search:{topic},"
        f"from_publication_date:{from_date}"
        f"&select=id,title,authorships,publication_year,primary_location,doi"
        f"&sort=publication_year:desc"
        f"&per_page=25"
        f"&mailto={MAILTO}"
    )

    return _fetch_and_store(alert["id"], url)


def _fetch_and_store(alert_id: int, url: str) -> int:
    """Fetch papers from OpenAlex and store new ones."""
    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(url, timeout=20)
        if resp.status_code != 200:
            return 0

        data = resp.json()
    except Exception as e:
        logger.error(f"Alert check failed: {e}")
        return 0

    new_count = 0
    conn = sqlite3.connect(DB_PATH)

    for work in data.get("results", []):
        paper_id = work.get("id", "")
        title = work.get("title", "")
        year = work.get("publication_year")
        doi = work.get("doi", "")

        authors = []
        for a in (work.get("authorships") or [])[:5]:
            name = (a.get("author") or {}).get("display_name", "")
            if name:
                authors.append(name)
        authors_str = ", ".join(authors[:3])
        if len(authors) > 3:
            authors_str += " et al."

        loc = work.get("primary_location") or {}
        source = loc.get("source") or {}
        journal = source.get("display_name", "")

        try:
            conn.execute(
                """INSERT INTO alert_results
                   (alert_id, paper_id, title, authors_str, year, journal, doi)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (alert_id, paper_id, title, authors_str, year, journal, doi)
            )
            new_count += 1
        except sqlite3.IntegrityError:
            pass  # Already tracked

    conn.commit()
    conn.close()
    return new_count


def check_all_alerts() -> dict:
    """Check all alerts for new papers. Returns summary."""
    alerts = get_alerts()
    results = {}
    for alert in alerts:
        count = check_alert(alert["id"])
        results[alert["id"]] = count
        time.sleep(0.3)  # rate limit
    return results


def search_authors(query: str, limit: int = 10) -> list:
    """Search for authors on OpenAlex to add as alerts."""
    url = (
        f"{BASE_URL}/authors?"
        f"search={query}"
        f"&select=id,display_name,works_count,cited_by_count,last_known_institutions"
        f"&per_page={limit}"
        f"&mailto={MAILTO}"
    )

    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return []

        results = []
        for author in resp.json().get("results", []):
            institutions = author.get("last_known_institutions") or []
            inst_name = institutions[0].get("display_name", "") if institutions else ""

            results.append({
                "id": author.get("id", ""),
                "name": author.get("display_name", ""),
                "works_count": author.get("works_count", 0),
                "cited_by_count": author.get("cited_by_count", 0),
                "institution": inst_name,
            })
        return results
    except Exception:
        return []
