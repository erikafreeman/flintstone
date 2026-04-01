"""Fix umlaut variants and propagate department info to all name variants of the same person."""

import os
import re
import sqlite3
import unicodedata
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")

# Common German umlaut ascii substitutions
UMLAUT_MAP = {
    'ae': 'ä', 'oe': 'ö', 'ue': 'ü', 'ss': 'ß',
    'Ae': 'Ä', 'Oe': 'Ö', 'Ue': 'Ü',
}

# Mojibake patterns (UTF-8 bytes read as Latin-1)
MOJIBAKE_MAP = {
    'Ã¤': 'ä', 'Ã¶': 'ö', 'Ã¼': 'ü', 'ÃŸ': 'ß',
    'Ã„': 'Ä', 'Ã–': 'Ö', 'Ãœ': 'Ü',
    'Ã©': 'é', 'Ã¨': 'è', 'Ã³': 'ó', 'Ã¡': 'á',
    'Ã­': 'í', 'Ã±': 'ñ', 'Ã§': 'ç', 'Å¡': 'š',
    'Å¾': 'ž', 'Ä‡': 'ć', 'Ä': 'č',
}


def normalize_for_matching(name: str) -> str:
    """Normalize a name for fuzzy matching — strip accents, lowercase, collapse whitespace."""
    # Fix mojibake first
    for bad, good in MOJIBAKE_MAP.items():
        name = name.replace(bad, good)

    # Normalize unicode
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))

    # Remove titles
    ascii_name = re.sub(r'\b(Dr|Prof|PD)\b\.?', '', ascii_name)

    # Lowercase, collapse spaces
    return re.sub(r'\s+', ' ', ascii_name).strip().lower()


def fix_mojibake_names(conn):
    """Fix names with mojibake encoding issues."""
    cur = conn.cursor()
    cur.execute("SELECT id, display_name FROM authors")
    fixed = 0
    for author_id, name in cur.fetchall():
        new_name = name
        for bad, good in MOJIBAKE_MAP.items():
            new_name = new_name.replace(bad, good)
        if new_name != name:
            cur.execute("UPDATE authors SET display_name = ? WHERE id = ?", (new_name, author_id))
            fixed += 1
    conn.commit()
    return fixed


def propagate_departments(conn):
    """For authors with multiple name variants, propagate department info from the matched one."""
    cur = conn.cursor()

    # Get all authors with department info
    cur.execute("""
        SELECT id, display_name, department, position
        FROM authors
        WHERE is_current_staff = 1 AND department IS NOT NULL AND department != ''
    """)
    staff_with_dept = cur.fetchall()

    # Build lookup by normalized name
    dept_lookup = {}
    for author_id, name, dept, pos in staff_with_dept:
        norm = normalize_for_matching(name)
        dept_lookup[norm] = (dept, pos)
        # Also store by last name for fallback
        parts = norm.split()
        if len(parts) >= 2:
            dept_lookup[parts[-1]] = (dept, pos)

    # Now find all IGB authors without department and try to match
    cur.execute("""
        SELECT DISTINCT a.id, a.display_name
        FROM authors a
        JOIN publication_authors pa ON a.id = pa.author_id
        WHERE pa.is_igb_affiliated = 1
          AND (a.is_current_staff = 0 OR a.is_current_staff IS NULL)
    """)
    unmatched = cur.fetchall()

    propagated = 0
    for author_id, name in unmatched:
        norm = normalize_for_matching(name)

        if norm in dept_lookup:
            dept, pos = dept_lookup[norm]
            cur.execute(
                "UPDATE authors SET department = ?, position = ?, is_current_staff = 1 WHERE id = ?",
                (dept, pos, author_id)
            )
            propagated += 1

    conn.commit()
    return propagated


def main():
    conn = sqlite3.connect(DB_PATH)

    print("Fixing mojibake names...")
    fixed = fix_mojibake_names(conn)
    print(f"  Fixed {fixed} names")

    print("Propagating department info to name variants...")
    propagated = propagate_departments(conn)
    print(f"  Propagated to {propagated} additional author records")

    # Summary
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM authors WHERE is_current_staff = 1")
    total_staff = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(DISTINCT a.id) FROM authors a
        JOIN publication_authors pa ON a.id = pa.author_id
        WHERE pa.is_igb_affiliated = 1 AND a.is_current_staff = 1
    """)
    igb_staff = cur.fetchone()[0]
    print(f"\nTotal matched to current staff: {total_staff} ({igb_staff} IGB-affiliated)")

    conn.close()


if __name__ == "__main__":
    main()
