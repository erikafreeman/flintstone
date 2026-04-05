"""Reading list management — save, annotate, and export papers."""

import os
import sqlite3
import csv
import io
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")


def _ensure_tables():
    """Create reading list tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reading_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reading_list_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            list_id INTEGER NOT NULL,
            publication_id TEXT NOT NULL,
            note TEXT DEFAULT '',
            tags TEXT DEFAULT '',
            added_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (list_id) REFERENCES reading_lists(id),
            UNIQUE(list_id, publication_id)
        )
    """)
    # Create default list if none exist
    count = conn.execute("SELECT COUNT(*) FROM reading_lists").fetchone()[0]
    if count == 0:
        conn.execute(
            "INSERT INTO reading_lists (name, description) VALUES (?, ?)",
            ("My Reading List", "Default reading list")
        )
    conn.commit()
    conn.close()


def get_lists() -> list:
    """Get all reading lists with item counts."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT rl.*, COUNT(rli.id) as item_count
        FROM reading_lists rl
        LEFT JOIN reading_list_items rli ON rl.id = rli.list_id
        GROUP BY rl.id
        ORDER BY rl.updated_at DESC
    """).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def create_list(name: str, description: str = "") -> int:
    """Create a new reading list. Returns the list ID."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "INSERT INTO reading_lists (name, description) VALUES (?, ?)",
        (name, description)
    )
    list_id = cur.lastrowid
    conn.commit()
    conn.close()
    return list_id


def get_list_items(list_id: int) -> list:
    """Get all items in a reading list with publication metadata."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT rli.*, p.title, p.abstract, p.year, p.journal, p.doi,
               p.cited_by_count, p.type
        FROM reading_list_items rli
        JOIN publications p ON rli.publication_id = p.id
        WHERE rli.list_id = ?
        ORDER BY rli.added_at DESC
    """, (list_id,)).fetchall()
    items = [dict(r) for r in rows]

    # Get authors for each
    for item in items:
        authors = conn.execute("""
            SELECT a.id, a.display_name, pa.is_igb_affiliated
            FROM authors a
            JOIN publication_authors pa ON a.id = pa.author_id
            WHERE pa.publication_id = ?
        """, (item["publication_id"],)).fetchall()
        item["authors"] = [dict(a) for a in authors]
        item["authors_str"] = ", ".join(
            a["display_name"] for a in item["authors"][:5]
        ) + (" et al." if len(item["authors"]) > 5 else "")

    conn.close()
    return items


def add_item(list_id: int, publication_id: str, note: str = "", tags: str = "") -> bool:
    """Add a paper to a reading list. Returns True if added, False if already exists."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO reading_list_items (list_id, publication_id, note, tags) VALUES (?, ?, ?, ?)",
            (list_id, publication_id, note, tags)
        )
        conn.execute(
            "UPDATE reading_lists SET updated_at = datetime('now') WHERE id = ?",
            (list_id,)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def remove_item(list_id: int, publication_id: str) -> bool:
    """Remove a paper from a reading list."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "DELETE FROM reading_list_items WHERE list_id = ? AND publication_id = ?",
        (list_id, publication_id)
    )
    conn.commit()
    removed = cur.rowcount > 0
    conn.close()
    return removed


def update_note(list_id: int, publication_id: str, note: str) -> bool:
    """Update the note for a reading list item."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "UPDATE reading_list_items SET note = ? WHERE list_id = ? AND publication_id = ?",
        (note, list_id, publication_id)
    )
    conn.commit()
    updated = cur.rowcount > 0
    conn.close()
    return updated


def update_tags(list_id: int, publication_id: str, tags: str) -> bool:
    """Update the tags for a reading list item."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "UPDATE reading_list_items SET tags = ? WHERE list_id = ? AND publication_id = ?",
        (tags, list_id, publication_id)
    )
    conn.commit()
    updated = cur.rowcount > 0
    conn.close()
    return updated


def export_csv(list_id: int) -> str:
    """Export a reading list as CSV string."""
    items = get_list_items(list_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Title", "Authors", "Year", "Journal", "DOI", "Citations", "Note", "Tags", "Added"])

    for item in items:
        writer.writerow([
            item.get("title", ""),
            item.get("authors_str", ""),
            item.get("year", ""),
            item.get("journal", ""),
            item.get("doi", ""),
            item.get("cited_by_count", 0),
            item.get("note", ""),
            item.get("tags", ""),
            item.get("added_at", ""),
        ])

    return output.getvalue()


def export_bibtex(list_id: int) -> str:
    """Export a reading list as BibTeX."""
    items = get_list_items(list_id)
    entries = []

    for item in items:
        # Generate citation key
        first_author = ""
        if item.get("authors"):
            first_author = item["authors"][0]["display_name"].split()[-1].lower()
        year = item.get("year", "")
        key = f"{first_author}{year}"

        # Clean title
        title = item.get("title", "").replace("{", "").replace("}", "")

        entry = f"""@article{{{key},
  title = {{{title}}},
  author = {{{item.get('authors_str', '')}}},
  year = {{{year}}},
  journal = {{{item.get('journal', '')}}},
  doi = {{{(item.get('doi') or '').replace('https://doi.org/', '')}}}
}}"""
        entries.append(entry)

    return "\n\n".join(entries)


def is_in_any_list(publication_id: str) -> list:
    """Check which reading lists contain this publication."""
    _ensure_tables()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT rl.id, rl.name
        FROM reading_list_items rli
        JOIN reading_lists rl ON rli.list_id = rl.id
        WHERE rli.publication_id = ?
    """, (publication_id,)).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result
