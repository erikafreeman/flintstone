"""Extract IGB research profile from Feuerstein's database."""

import json
import os
import sqlite3
from collections import Counter, defaultdict

FLINTSTONE_DB = os.environ.get(
    "FLINTSTONE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "flinstone", "data", "flinstone.db"),
)

DEPARTMENTS = {
    "(Dept. 1) Ecohydrology and Biogeochemistry": "dept1",
    "(Dept. 2) Community and Ecosystem Ecology": "dept2",
    "(Dept. 3) Plankton and Microbial Ecology": "dept3",
    "(Dept. 4) Fish Biology, Fisheries and Aquaculture": "dept4",
    "(Dept. 5) Evolutionary and Integrative Ecology": "dept5",
}


def extract_profile(output_path: str = None) -> dict:
    """Read Feuerstein's DB and build an IGB research profile.

    Returns a dict with:
      - institute_concepts: [{concept_name, paper_count, avg_score}, ...]
      - department_concepts: {dept_name: [{concept_name, count, avg_score}, ...]}
      - igb_pub_ids: list of all IGB publication OpenAlex IDs
      - igb_author_ids: list of IGB-affiliated author IDs
      - openalex_concept_ids: list of concept IDs for OpenAlex filtering (if available)
    """
    if not os.path.exists(FLINTSTONE_DB):
        print(f"Feuerstein DB not found at {FLINTSTONE_DB}")
        return _empty_profile()

    conn = sqlite3.connect(FLINTSTONE_DB)
    conn.row_factory = sqlite3.Row

    # Institute-wide concepts
    cur = conn.execute("""
        SELECT concept_name, COUNT(*) as cnt, AVG(score) as avg_score
        FROM concepts
        GROUP BY concept_name
        ORDER BY cnt DESC
    """)
    institute_concepts = [
        {"concept_name": r[0], "paper_count": r[1], "avg_score": round(r[2], 3)}
        for r in cur.fetchall()
    ]

    # Department-level concepts: join concepts -> publications -> pub_authors -> authors
    dept_concepts = defaultdict(Counter)
    dept_concept_scores = defaultdict(lambda: defaultdict(list))

    cur = conn.execute("""
        SELECT c.concept_name, c.score, a.department
        FROM concepts c
        JOIN publication_authors pa ON c.publication_id = pa.publication_id
        JOIN authors a ON pa.author_id = a.id
        WHERE pa.is_igb_affiliated = 1
          AND a.department IS NOT NULL
          AND a.department != ''
    """)
    for row in cur.fetchall():
        concept, score, dept = row[0], row[1], row[2]
        dept_concepts[dept][concept] += 1
        dept_concept_scores[dept][concept].append(score)

    department_profiles = {}
    for dept_name, concept_counts in dept_concepts.items():
        top = concept_counts.most_common(100)
        department_profiles[dept_name] = [
            {
                "concept_name": name,
                "count": count,
                "avg_score": round(
                    sum(dept_concept_scores[dept_name][name]) / len(dept_concept_scores[dept_name][name]), 3
                ),
            }
            for name, count in top
        ]

    # IGB publication IDs
    cur = conn.execute("SELECT id FROM publications")
    igb_pub_ids = [r[0] for r in cur.fetchall()]

    # IGB author IDs (current staff)
    cur = conn.execute(
        "SELECT id FROM authors WHERE is_current_staff = 1"
    )
    igb_author_ids = [r[0] for r in cur.fetchall()]

    conn.close()

    profile = {
        "institute_concepts": institute_concepts[:200],
        "department_concepts": department_profiles,
        "igb_pub_ids": igb_pub_ids,
        "igb_author_ids": igb_author_ids,
        "top_concept_names": [c["concept_name"] for c in institute_concepts[:100]],
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(profile, f, indent=2, ensure_ascii=False)
        print(f"Profile saved to {output_path}")
        print(f"  Institute concepts: {len(institute_concepts)}")
        print(f"  Departments: {len(department_profiles)}")
        print(f"  IGB publications: {len(igb_pub_ids)}")
        print(f"  IGB authors (current staff): {len(igb_author_ids)}")

    return profile


def load_profile(path: str = None) -> dict:
    """Load a cached profile from JSON."""
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "..", "data", "igb_profile.json")
    if not os.path.exists(path):
        return _empty_profile()
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _empty_profile():
    return {
        "institute_concepts": [],
        "department_concepts": {},
        "igb_pub_ids": [],
        "igb_author_ids": [],
        "top_concept_names": [],
    }
