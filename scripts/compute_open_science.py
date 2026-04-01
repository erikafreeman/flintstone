"""
Open Practices Score — v2

Computes the IGB Open Practices Score using:
  - 5 dimensions (OA accessibility, data openness, code openness, preprint sharing, repository licensing)
  - Empirical Bayes shrinkage (no volume multiplier)
  - 3-tier evidence system (verified / strong proxy / inferred)
  - Per-publication max-one-contribution-per-dimension rule
  - Confidence labels based on publication count

Data sources: OpenAlex (via SQLite), FRED/DataCite (fred_downloadable.json, fred_creators.json), abstract text
"""

import sqlite3
import json
import math
import re
import sys
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "flinstone.db"
FRED_DL_PATH = BASE / "data" / "fred_downloadable.json"
FRED_CREATORS_PATH = BASE / "data" / "fred_creators.json"
COMPONENTS_PATH = BASE / "data" / "opensci_components.json"
OUTPUT_PATH = BASE / "app" / "static" / "open_science.json"

# ── Shrinkage constant ──────────────────────────────────────────────
K = 10  # smoothing constant for empirical Bayes

# ── Dimension weights ────────────────────────────────────────────────
WEIGHTS = {
    "oa_accessibility": 0.30,
    "data_openness":    0.25,
    "code_openness":    0.20,
    "preprint_sharing": 0.10,
    "repo_licensing":   0.15,
}

# ── OA per-publication scores ────────────────────────────────────────
OA_SCORES = {
    "gold":    1.0,
    "diamond": 1.0,
    "hybrid":  0.9,
    "green":   0.7,
    "bronze":  0.2,
    "closed":  0.0,
}

# ── Code detection patterns (Tier 1 vs Tier 3) ──────────────────────
CODE_TIER1_PATTERNS = [
    r'10\.\d{4,}/zenodo\.\d+',        # Zenodo DOI for software
    r'cran\.r-project\.org',
    r'pypi\.org/project/',
]
CODE_TIER3_PATTERNS = [
    r'github\.com/\S+',
    r'gitlab\.com/\S+',
    r'bitbucket\.org/\S+',
    r'codeberg\.org/\S+',
    r'\bsource\s+code\b',
    r'\bcode\s+(?:is\s+)?(?:available|accessible)\b',
    r'\br\s+package\b',
    r'\bpython\s+package\b',
]

# ── Data detection patterns ──────────────────────────────────────────
DATA_TIER1_PATTERNS = [
    r'10\.\d{4,}/dryad\.',
    r'10\.\d{4,}/zenodo\.\d+',
    r'10\.\d{4,}/pangaea\.',
    r'10\.\d{4,}/figshare\.',
    r'doi\.org/10\.\d{4,}/',           # generic DOI in data context
]
DATA_TIER3_STMT_PATTERNS = [
    r'data\s+(?:are\s+)?(?:available|accessible|deposited)\s+(?:at|from|in|on|via)',
    r'data\s+availability\s+statement',
    r'data\s+availability:',
    r'deposited\s+(?:in|at|on)\s+\S*(?:dryad|zenodo|figshare|pangaea|genbank|ncbi)',
]
DATA_TIER3_REQUEST_PATTERNS = [
    r'(?:data|materials?)\s+(?:are\s+)?available\s+(?:upon|on)\s+(?:reasonable\s+)?request',
]

# ── Unicode normalization ────────────────────────────────────────────
SPECIAL_HYPHENS = '\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D'

UMLAUT_MAP = {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss',
               'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'}

def normalize_name(name):
    for h in SPECIAL_HYPHENS:
        name = name.replace(h, '-')
    return name

def merge_key(name):
    """Canonical key for merging: lowercase, umlauts normalized, hyphens normalized."""
    name = normalize_name(name).lower()
    for k, v in UMLAUT_MAP.items():
        name = name.replace(k.lower(), v.lower())
    return name


def shrink(observed_rate, n, igb_mean, k=K):
    """Empirical Bayes shrinkage: pull observed rate toward IGB mean for small n."""
    return (n / (n + k)) * observed_rate + (k / (n + k)) * igb_mean


def confidence_label(n):
    if n >= 30: return "high"
    if n >= 10: return "medium"
    return "low"


def score_pub_oa(oa_type):
    """Per-publication OA score (Tier 1-2: verified metadata)."""
    return OA_SCORES.get(oa_type, 0.0)


def score_pub_data(abstract, pub_type):
    """Per-publication data openness score. Returns max of applicable tiers."""
    if not abstract:
        abstract = ""
    abstract_lower = abstract.lower()

    # Tier 1: verified dataset DOI linked in abstract
    if pub_type == "dataset":
        return 1.0
    for pat in DATA_TIER1_PATTERNS:
        if re.search(pat, abstract_lower):
            return 1.0

    # Tier 2: dataset type in OpenAlex (already captured by pub_type check above)

    # Tier 3: data availability statement
    for pat in DATA_TIER3_STMT_PATTERNS:
        if re.search(pat, abstract_lower):
            return 0.3

    # Tier 3 (lowest): "available upon request"
    for pat in DATA_TIER3_REQUEST_PATTERNS:
        if re.search(pat, abstract_lower):
            return 0.1

    return 0.0


def score_pub_code(abstract):
    """Per-publication code openness score. Returns max of applicable tiers."""
    if not abstract:
        return 0.0
    abstract_lower = abstract.lower()

    # Tier 1: verified software DOI / package record
    for pat in CODE_TIER1_PATTERNS:
        if re.search(pat, abstract_lower):
            return 1.0

    # Tier 3: repository link in abstract
    for pat in CODE_TIER3_PATTERNS[:4]:  # github/gitlab/bitbucket/codeberg
        if re.search(pat, abstract_lower):
            return 0.5

    # Tier 3: generic code mention
    for pat in CODE_TIER3_PATTERNS[4:]:
        if re.search(pat, abstract_lower):
            return 0.2

    return 0.0


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    # ── Load FRED data ───────────────────────────────────────────────
    fred_dl = json.load(open(FRED_DL_PATH, encoding="utf-8"))
    fred_creators = json.load(open(FRED_CREATORS_PATH, encoding="utf-8"))

    # Build author_name -> {downloadable: N, restricted: N} from FRED
    fred_by_creator = defaultdict(lambda: {"downloadable": 0, "restricted": 0, "total": 0})
    for ds in fred_creators.get("datasets", []):
        doi = ds["doi"]
        is_dl = fred_dl.get(doi, False)
        for creator in ds.get("creators", []):
            name_norm = normalize_name(creator.strip())
            fred_by_creator[name_norm]["total"] += 1
            if is_dl:
                fred_by_creator[name_norm]["downloadable"] += 1
            else:
                fred_by_creator[name_norm]["restricted"] += 1

    # ── Get all current staff ────────────────────────────────────────
    staff = db.execute("""
        SELECT id, display_name, department
        FROM authors WHERE is_current_staff = 1
    """).fetchall()

    # Build merge_key -> list of IDs (for merging split OpenAlex authors)
    # Keep best display name (the one with most pubs)
    name_to_ids = defaultdict(list)
    name_to_dept = {}
    name_to_display = {}  # merge_key -> best display name
    for s in staff:
        display = normalize_name(s["display_name"])
        key = merge_key(display)
        name_to_ids[key].append(s["id"])
        if s["department"]:
            name_to_dept[key] = s["department"]
        # Prefer the name with special characters (Dörthe > Doerthe)
        if key not in name_to_display or any(ord(c) > 127 for c in display):
            name_to_display[key] = display

    # ── Compute per-publication scores for each author ───────────────
    print(f"Computing scores for {len(name_to_ids)} unique staff members...")

    # First pass: compute IGB-wide means for shrinkage
    all_oa_scores = []
    all_data_scores = []
    all_code_scores = []
    all_preprint_rates = []
    all_repo_rates = []

    author_results = {}

    for mkey, author_ids in name_to_ids.items():
        name = name_to_display.get(mkey, mkey)
        placeholders = ",".join(["?"] * len(author_ids))

        # Get all publications for this author (deduplicated)
        pubs = db.execute(f"""
            SELECT DISTINCT p.id, p.title, p.abstract, p.year, p.doi, p.type,
                   p.is_oa, p.oa_type, p.cited_by_count, p.journal
            FROM publications p
            JOIN publication_authors pa ON pa.publication_id = p.id
            WHERE pa.author_id IN ({placeholders})
            ORDER BY p.year DESC
        """, author_ids).fetchall()

        if len(pubs) < 3:
            continue

        # ── Per-pub scoring ──────────────────────────────────────────
        oa_scores_list = []
        data_scores_list = []
        code_scores_list = []
        eligible_articles = 0
        preprint_count = 0

        # Detailed counts for display
        gold = hybrid = green = bronze = diamond = closed = 0
        data_verified = data_statement = data_request = 0
        code_verified = code_repo = code_mention = 0

        for pub in pubs:
            oa_type = pub["oa_type"] or "closed"
            pub_type = pub["type"] or ""
            abstract = pub["abstract"] or ""

            # OA dimension
            oa_s = score_pub_oa(oa_type)
            oa_scores_list.append(oa_s)
            if oa_type == "gold": gold += 1
            elif oa_type == "diamond": diamond += 1
            elif oa_type == "hybrid": hybrid += 1
            elif oa_type == "green": green += 1
            elif oa_type == "bronze": bronze += 1
            else: closed += 1

            # Data dimension (FRED excluded — it goes to repo dimension)
            d_s = score_pub_data(abstract, pub_type)
            data_scores_list.append(d_s)
            if d_s >= 1.0: data_verified += 1
            elif d_s >= 0.3: data_statement += 1
            elif d_s >= 0.1: data_request += 1

            # Code dimension
            c_s = score_pub_code(abstract)
            code_scores_list.append(c_s)
            if c_s >= 1.0: code_verified += 1
            elif c_s >= 0.5: code_repo += 1
            elif c_s >= 0.2: code_mention += 1

            # Preprint dimension
            if pub_type in ("article", "review", "letter"):
                eligible_articles += 1
            if pub_type == "preprint":
                preprint_count += 1

        n_pubs = len(pubs)

        # Raw rates (0-100)
        oa_rate = sum(oa_scores_list) / n_pubs * 100 if n_pubs else 0
        data_rate = sum(data_scores_list) / n_pubs * 100 if n_pubs else 0
        code_rate = sum(code_scores_list) / n_pubs * 100 if n_pubs else 0
        preprint_rate = min(preprint_count / max(eligible_articles, 1) * 100, 100)

        # FRED / Repository licensing dimension
        # Match by name (FRED uses creator names, not OpenAlex IDs)
        fred_info = {"downloadable": 0, "restricted": 0, "total": 0}
        # Try matching by last name, first name
        for creator_name, fdata in fred_by_creator.items():
            # Match: last name from staff name appears in FRED creator name
            name_parts = name.split()
            if len(name_parts) >= 2:
                last = name_parts[-1].lower()
                first = name_parts[0].lower()
                creator_lower = creator_name.lower()
                if last in creator_lower and (first[0] in creator_lower.split(",")[0] if "," in creator_lower else first[:3] in creator_lower):
                    fred_info["downloadable"] += fdata["downloadable"]
                    fred_info["restricted"] += fdata["restricted"]
                    fred_info["total"] += fdata["total"]

        fred_total = fred_info["total"]
        repo_rate = (fred_info["downloadable"] / fred_total * 100) if fred_total > 0 else None  # None = no deposits

        # Store for IGB mean computation
        all_oa_scores.append(oa_rate)
        all_data_scores.append(data_rate)
        all_code_scores.append(code_rate)
        all_preprint_rates.append(preprint_rate)
        if repo_rate is not None:
            all_repo_rates.append(repo_rate)

        author_results[name] = {
            "ids": author_ids,
            "dept": name_to_dept.get(mkey, ""),
            "n_pubs": n_pubs,
            "oa_rate": oa_rate,
            "data_rate": data_rate,
            "code_rate": code_rate,
            "preprint_rate": preprint_rate,
            "repo_rate": repo_rate,
            "fred_total": fred_total,
            "fred_downloadable": fred_info["downloadable"],
            "fred_restricted": fred_info["restricted"],
            "eligible_articles": eligible_articles,
            # Display counts
            "gold": gold, "diamond": diamond, "hybrid": hybrid,
            "green": green, "bronze": bronze, "closed": closed,
            "data_verified": data_verified, "data_statement": data_statement,
            "data_request": data_request,
            "code_verified": code_verified, "code_repo": code_repo,
            "code_mention": code_mention,
            "preprints": preprint_count,
            "oa_pubs": n_pubs - closed,
            "citations": sum(p["cited_by_count"] or 0 for p in pubs),
            # Per-pub lists for yearly breakdown
            "_pubs": [dict(p) for p in pubs],
        }

    # ── Compute IGB means ────────────────────────────────────────────
    igb_means = {
        "oa": sum(all_oa_scores) / len(all_oa_scores) if all_oa_scores else 50,
        "data": sum(all_data_scores) / len(all_data_scores) if all_data_scores else 10,
        "code": sum(all_code_scores) / len(all_code_scores) if all_code_scores else 5,
        "preprint": sum(all_preprint_rates) / len(all_preprint_rates) if all_preprint_rates else 8,
        "repo": sum(all_repo_rates) / len(all_repo_rates) if all_repo_rates else 25,
    }

    print(f"\nIGB means (pre-shrinkage):")
    for k, v in igb_means.items():
        print(f"  {k}: {v:.1f}%")

    # ── Apply shrinkage and compute final scores ─────────────────────
    output = []

    for name, r in author_results.items():
        n = r["n_pubs"]

        # Shrinkage-adjusted rates
        adj_oa = shrink(r["oa_rate"], n, igb_means["oa"])
        adj_data = shrink(r["data_rate"], n, igb_means["data"])
        adj_code = shrink(r["code_rate"], n, igb_means["code"])
        adj_preprint = shrink(r["preprint_rate"], max(r["eligible_articles"], 1), igb_means["preprint"])

        # Repo: use IGB mean if no FRED deposits (shrinkage handles this naturally)
        if r["repo_rate"] is not None:
            adj_repo = shrink(r["repo_rate"], r["fred_total"], igb_means["repo"])
        else:
            adj_repo = igb_means["repo"]  # no data -> IGB mean

        # Final composite score
        score = (
            WEIGHTS["oa_accessibility"] * adj_oa +
            WEIGHTS["data_openness"]    * adj_data +
            WEIGHTS["code_openness"]    * adj_code +
            WEIGHTS["preprint_sharing"] * adj_preprint +
            WEIGHTS["repo_licensing"]   * adj_repo
        )

        # ── Yearly breakdown ─────────────────────────────────────────
        yearly = {}
        for year in [2024, 2025, 2026]:
            year_pubs = [p for p in r["_pubs"] if p["year"] == year]
            if not year_pubs:
                continue

            ny = len(year_pubs)
            y_oa = sum(score_pub_oa(p["oa_type"] or "closed") for p in year_pubs) / ny * 100
            y_data = sum(score_pub_data(p["abstract"] or "", p["type"] or "") for p in year_pubs) / ny * 100
            y_code = sum(score_pub_code(p["abstract"] or "") for p in year_pubs) / ny * 100
            y_eligible = sum(1 for p in year_pubs if (p["type"] or "") in ("article", "review", "letter"))
            y_preprints = sum(1 for p in year_pubs if (p["type"] or "") == "preprint")
            y_preprint_rate = min(y_preprints / max(y_eligible, 1) * 100, 100)

            # Shrink yearly rates too
            y_adj_oa = shrink(y_oa, ny, igb_means["oa"])
            y_adj_data = shrink(y_data, ny, igb_means["data"])
            y_adj_code = shrink(y_code, ny, igb_means["code"])
            y_adj_preprint = shrink(y_preprint_rate, max(y_eligible, 1), igb_means["preprint"])

            y_score = (
                WEIGHTS["oa_accessibility"] * y_adj_oa +
                WEIGHTS["data_openness"]    * y_adj_data +
                WEIGHTS["code_openness"]    * y_adj_code +
                WEIGHTS["preprint_sharing"] * y_adj_preprint +
                WEIGHTS["repo_licensing"]   * adj_repo  # same repo score (not year-specific)
            )

            y_gold = sum(1 for p in year_pubs if (p["oa_type"] or "") == "gold")
            y_hybrid = sum(1 for p in year_pubs if (p["oa_type"] or "") == "hybrid")
            y_green = sum(1 for p in year_pubs if (p["oa_type"] or "") == "green")
            y_closed = sum(1 for p in year_pubs if not p["is_oa"])

            yearly[str(year)] = {
                "year": year,
                "total_pubs": ny,
                "oa_rate": round(100 * (ny - y_closed) / ny),
                "oa_adj": round(y_adj_oa, 1),
                "data_adj": round(y_adj_data, 1),
                "code_adj": round(y_adj_code, 1),
                "preprint_adj": round(y_adj_preprint, 1),
                "gold_oa": y_gold,
                "hybrid_oa": y_hybrid,
                "green_oa": y_green,
                "data_rate": round(y_data, 1),
                "code_rate": round(y_code, 1),
                "preprint_rate": round(y_preprint_rate, 1),
                "score": round(y_score, 1),
            }

        entry = {
            "id": r["ids"][0],  # primary OpenAlex ID
            "name": name,
            "dept": r["dept"],
            "total_pubs": n,
            "confidence": confidence_label(n),
            # Final score
            "score": round(score, 1),
            # Adjusted (shrinkage) rates
            "adj_oa": round(adj_oa, 1),
            "adj_data": round(adj_data, 1),
            "adj_code": round(adj_code, 1),
            "adj_preprint": round(adj_preprint, 1),
            "adj_repo": round(adj_repo, 1),
            # Raw observed rates (for transparency)
            "raw_oa": round(r["oa_rate"], 1),
            "raw_data": round(r["data_rate"], 1),
            "raw_code": round(r["code_rate"], 1),
            "raw_preprint": round(r["preprint_rate"], 1),
            "raw_repo": round(r["repo_rate"], 1) if r["repo_rate"] is not None else None,
            # Counts for display
            "gold": r["gold"], "diamond": r["diamond"], "hybrid": r["hybrid"],
            "green": r["green"], "bronze": r["bronze"], "closed": r["closed"],
            "oa_pubs": r["oa_pubs"],
            "oa_rate": round(100 * r["oa_pubs"] / n),
            "data_verified": r["data_verified"],
            "data_statement": r["data_statement"],
            "data_request": r["data_request"],
            "code_verified": r["code_verified"],
            "code_repo": r["code_repo"],
            "code_mention": r["code_mention"],
            "preprints": r["preprints"],
            "fred_downloadable": r["fred_downloadable"],
            "fred_restricted": r["fred_restricted"],
            "fred_total": r["fred_total"],
            "citations": r["citations"],
            # Yearly
            "yearly": yearly,
        }
        output.append(entry)

    # Sort by score descending
    output.sort(key=lambda x: x["score"], reverse=True)

    # Save
    json.dump(output, open(OUTPUT_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"\n✓ Saved {len(output)} researchers to {OUTPUT_PATH}")
    print(f"\nTop 10:")
    for i, r in enumerate(output[:10]):
        print(f"  {i+1}. {r['name']}: {r['score']} ({r['confidence']} confidence, {r['total_pubs']} pubs)")
        print(f"     OA={r['adj_oa']:.0f} Data={r['adj_data']:.0f} Code={r['adj_code']:.0f} Pre={r['adj_preprint']:.0f} Repo={r['adj_repo']:.0f}")

    # IGB-wide stats
    print(f"\nIGB-wide stats:")
    print(f"  Researchers scored: {len(output)}")
    print(f"  High confidence: {sum(1 for r in output if r['confidence']=='high')}")
    print(f"  Medium confidence: {sum(1 for r in output if r['confidence']=='medium')}")
    print(f"  Low confidence: {sum(1 for r in output if r['confidence']=='low')}")
    avg_score = sum(r["score"] for r in output) / len(output)
    print(f"  Average score: {avg_score:.1f}")


if __name__ == "__main__":
    main()
