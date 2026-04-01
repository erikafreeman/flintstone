"""Scrape IGB staff directory to get department and position info for authors."""

import os
import re
import sqlite3
import time
import requests

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")
BASE_URL = "https://www.igb-berlin.de/en/people"
TOTAL_PAGES = 36


def parse_staff_page(html_text: str) -> list:
    """Parse staff listing page using regex."""
    import html as html_mod
    staff = []

    blocks = re.split(r'node--type-person\s+node--view-mode-teaser', html_text)

    for block in blocks[1:]:
        person = {}

        name_match = re.search(r'<h2>([^<]+)</h2>', block)
        if name_match:
            person["name"] = html_mod.unescape(name_match.group(1).strip())
        else:
            continue

        dept_match = re.search(
            r'field--name-field-department.*?field__item[^>]*>([^<]+)</div>',
            block, re.DOTALL,
        )
        person["department"] = dept_match.group(1).strip() if dept_match else ""

        pos_match = re.search(
            r'field--name-field-bereich-position.*?field__item[^>]*>([^<]+)</div>',
            block, re.DOTALL,
        )
        person["position"] = pos_match.group(1).strip() if pos_match else ""

        staff.append(person)

    return staff


def normalize_name(name: str) -> str:
    """Normalize a name for fuzzy matching."""
    name = re.sub(r'\b(Dr|Prof|PD)\b\.?', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()


def fetch_all_staff():
    """Fetch all staff from IGB website."""
    all_staff = []

    for page in range(TOTAL_PAGES):
        url = f"{BASE_URL}?page={page}"
        print(f"  Fetching page {page + 1}/{TOTAL_PAGES}...")
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Flintstone/1.0 IGB-internal-tool"})
        resp.raise_for_status()

        staff = parse_staff_page(resp.text)
        all_staff.extend(staff)
        time.sleep(0.3)

    return all_staff


def match_staff_to_authors(conn: sqlite3.Connection, staff: list):
    """Match IGB website staff to OpenAlex authors by name."""
    cur = conn.cursor()

    # Add columns if they don't exist
    for col in ["department TEXT", "position TEXT", "is_current_staff INTEGER DEFAULT 0"]:
        try:
            cur.execute(f"ALTER TABLE authors ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    # Reset previous matches
    cur.execute("UPDATE authors SET department = NULL, position = NULL, is_current_staff = 0")

    # Build lookup from normalized name -> staff info
    staff_lookup = {}
    for s in staff:
        norm = normalize_name(s["name"])
        staff_lookup[norm] = s

    # Get all IGB-affiliated authors
    cur.execute("""
        SELECT DISTINCT a.id, a.display_name
        FROM authors a
        JOIN publication_authors pa ON a.id = pa.author_id
        WHERE pa.is_igb_affiliated = 1
    """)
    authors = cur.fetchall()

    matched = 0
    for author_id, display_name in authors:
        norm = normalize_name(display_name)

        # Try exact match
        if norm in staff_lookup:
            s = staff_lookup[norm]
            cur.execute(
                "UPDATE authors SET department = ?, position = ?, is_current_staff = 1 WHERE id = ?",
                (s["department"], s["position"], author_id),
            )
            matched += 1
            continue

        # Try partial match: same last name + first 3 chars of first name
        parts = norm.split()
        if len(parts) >= 2:
            for key, s in staff_lookup.items():
                key_parts = key.split()
                if len(key_parts) >= 2:
                    if parts[-1] == key_parts[-1] and parts[0][:3] == key_parts[0][:3]:
                        cur.execute(
                            "UPDATE authors SET department = ?, position = ?, is_current_staff = 1 WHERE id = ?",
                            (s["department"], s["position"], author_id),
                        )
                        matched += 1
                        break

    conn.commit()
    return matched, len(authors)


def main():
    print("Fetching IGB staff directory...")
    staff = fetch_all_staff()
    print(f"Found {len(staff)} staff members")

    for s in staff[:5]:
        print(f"  {s['name']} | {s['department']} | {s['position']}")

    print("\nMatching to OpenAlex authors...")
    conn = sqlite3.connect(DB_PATH)
    matched, total = match_staff_to_authors(conn, staff)
    print(f"Matched {matched} of {total} IGB authors to current staff")

    # Show department distribution
    cur = conn.cursor()
    cur.execute("""
        SELECT department, COUNT(*)
        FROM authors
        WHERE is_current_staff = 1 AND department IS NOT NULL AND department != ''
        GROUP BY department
        ORDER BY COUNT(*) DESC
    """)
    print("\nDepartment distribution:")
    for dept, count in cur.fetchall():
        print(f"  {dept}: {count}")

    conn.close()


if __name__ == "__main__":
    main()
