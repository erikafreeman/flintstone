"""Incremental update: fetch only new/updated IGB publications since last refresh.

Usage:
    python scripts/update.py              # fetch new papers since last update
    python scripts/update.py --full       # full re-fetch of everything
    python scripts/update.py --since 2026-01-01  # fetch papers created/updated since date
"""

import argparse
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta

import numpy as np
import onnxruntime as ort
import requests
from tokenizers import Tokenizer

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, "..")
DB_PATH = os.path.join(PROJECT_DIR, "data", "flinstone.db")
EMBEDDINGS_PATH = os.path.join(PROJECT_DIR, "data", "embeddings.npy")
PUB_IDS_PATH = os.path.join(PROJECT_DIR, "data", "pub_ids.npy")
MODEL_DIR = os.path.join(PROJECT_DIR, "data", "model")
ONNX_PATH = os.path.join(MODEL_DIR, "onnx", "model.onnx")
TOKENIZER_PATH = os.path.join(MODEL_DIR, "tokenizer.json")
LAST_UPDATE_PATH = os.path.join(PROJECT_DIR, "data", ".last_update")

BASE_URL = "https://api.openalex.org"
IGB_ID = "I4210116314"
MAILTO = "erika.freeman@igb-berlin.de"


def get_last_update_date() -> str:
    """Read the last update timestamp, or return a default 30 days ago."""
    if os.path.exists(LAST_UPDATE_PATH):
        with open(LAST_UPDATE_PATH) as f:
            return f.read().strip()
    return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")


def save_last_update_date():
    """Save today's date as the last update."""
    with open(LAST_UPDATE_PATH, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


def reconstruct_abstract(inverted_index: dict) -> str:
    if not inverted_index:
        return ""
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)


def fetch_works_since(since_date: str):
    """Fetch IGB works created or updated since a given date."""
    cursor = "*"
    all_works = []
    page = 0

    print(f"  Fetching works updated since {since_date}...")

    while cursor:
        url = (
            f"{BASE_URL}/works?"
            f"filter=institutions.id:{IGB_ID},"
            f"from_updated_date:{since_date}"
            f"&per_page=200"
            f"&cursor={cursor}"
            f"&mailto={MAILTO}"
        )

        # Retry with backoff on rate limiting
        for attempt in range(5):
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            print("  Too many retries, stopping.")
            break

        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        all_works.extend(results)
        page += 1
        count = len(all_works)
        total = data.get("meta", {}).get("count", "?")
        print(f"  Page {page}: {len(results)} works ({count}/{total} total)")

        cursor = data.get("meta", {}).get("next_cursor")
        time.sleep(0.1)

    return all_works


def store_works(conn, works):
    """Store works in the database (INSERT OR REPLACE)."""
    new_count = 0
    updated_count = 0

    for work in works:
        work_id = work.get("id", "")
        title = work.get("title", "")
        abstract_inv = work.get("abstract_inverted_index")
        abstract = reconstruct_abstract(abstract_inv) if abstract_inv else ""
        # Strip any HTML tags
        abstract = re.sub(r'<[^>]+>', '', abstract)
        year = work.get("publication_year")
        doi = work.get("doi", "")
        primary_loc = work.get("primary_location") or {}
        source = primary_loc.get("source") or {}
        journal = source.get("display_name", "")
        cited_by = work.get("cited_by_count", 0)
        work_type = work.get("type", "")

        # Check if it already exists
        cur = conn.execute("SELECT id FROM publications WHERE id = ?", (work_id,))
        exists = cur.fetchone() is not None

        conn.execute(
            "INSERT OR REPLACE INTO publications VALUES (?,?,?,?,?,?,?,?)",
            (work_id, title, abstract, year, doi, journal, cited_by, work_type),
        )

        if exists:
            updated_count += 1
        else:
            new_count += 1

        # Authors
        for authorship in work.get("authorships", []):
            author = authorship.get("author") or {}
            author_id = author.get("id", "")
            if not author_id:
                continue
            display_name = author.get("display_name", "")
            orcid = author.get("orcid", "")

            conn.execute(
                "INSERT OR REPLACE INTO authors(id, display_name, orcid) VALUES (?,?,?)",
                (author_id, display_name, orcid),
            )

            institutions = authorship.get("institutions", [])
            is_igb = any(IGB_ID in (inst.get("id", "") or "") for inst in institutions)

            conn.execute(
                "INSERT OR REPLACE INTO publication_authors VALUES (?,?,?)",
                (work_id, author_id, int(is_igb)),
            )

    conn.commit()
    return new_count, updated_count


def rebuild_embeddings_incremental(conn):
    """Rebuild embeddings only for publications that don't have them yet."""
    # Load existing embeddings
    if os.path.exists(EMBEDDINGS_PATH) and os.path.exists(PUB_IDS_PATH):
        existing_embs = np.load(EMBEDDINGS_PATH)
        existing_ids = set(np.load(PUB_IDS_PATH, allow_pickle=True).tolist())
    else:
        existing_embs = np.zeros((0, 384), dtype=np.float32)
        existing_ids = set()

    # Find publications that need embedding
    cur = conn.cursor()
    cur.execute(
        "SELECT id, title, abstract FROM publications WHERE abstract != '' AND abstract IS NOT NULL"
    )
    all_rows = cur.fetchall()
    all_ids = [r[0] for r in all_rows]

    new_rows = [(r[0], r[1], r[2]) for r in all_rows if r[0] not in existing_ids]

    if not new_rows:
        print("  No new publications to embed")
        return 0

    print(f"  Embedding {len(new_rows)} new publications...")

    # Load model
    session = ort.InferenceSession(ONNX_PATH)
    tokenizer = Tokenizer.from_file(TOKENIZER_PATH)

    texts = [f"{r[1] or ''} {r[2] or ''}" for r in new_rows]
    new_ids = [r[0] for r in new_rows]

    # Encode in batches
    all_new_embs = []
    batch_size = 32
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        encodings = [tokenizer.encode(t) for t in batch]
        max_len = min(max(len(e.ids) for e in encodings), 256)

        input_ids, attention_mask, token_type_ids = [], [], []
        for enc in encodings:
            ids = enc.ids[:max_len]
            mask = enc.attention_mask[:max_len]
            ttype = enc.type_ids[:max_len]
            pad = max_len - len(ids)
            input_ids.append(ids + [0] * pad)
            attention_mask.append(mask + [0] * pad)
            token_type_ids.append(ttype + [0] * pad)

        input_ids = np.array(input_ids, dtype=np.int64)
        attn = np.array(attention_mask, dtype=np.int64)
        ttypes = np.array(token_type_ids, dtype=np.int64)

        outputs = session.run(None, {
            "input_ids": input_ids, "attention_mask": attn, "token_type_ids": ttypes
        })

        # Mean pooling + normalize
        mask_exp = np.expand_dims(attn, -1).astype(np.float32)
        summed = np.sum(outputs[0] * mask_exp, axis=1)
        counted = np.clip(np.sum(mask_exp, axis=1), 1e-9, None)
        embs = summed / counted
        norms = np.linalg.norm(embs, axis=1, keepdims=True)
        norms[norms == 0] = 1
        embs = embs / norms

        all_new_embs.append(embs)

    new_embs = np.vstack(all_new_embs).astype(np.float32)

    # Merge with existing
    combined_embs = np.vstack([existing_embs, new_embs])

    # Rebuild the full ID list in the same order
    existing_ids_list = np.load(PUB_IDS_PATH, allow_pickle=True).tolist() if os.path.exists(PUB_IDS_PATH) else []
    combined_ids = existing_ids_list + new_ids

    np.save(EMBEDDINGS_PATH, combined_embs)
    np.save(PUB_IDS_PATH, np.array(combined_ids))

    print(f"  Embeddings: {existing_embs.shape[0]} existing + {new_embs.shape[0]} new = {combined_embs.shape[0]} total")
    return len(new_rows)


def rebuild_fts(conn):
    """Rebuild the full-text search index."""
    print("  Rebuilding FTS index...")
    conn.execute("DROP TABLE IF EXISTS publications_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE publications_fts USING fts5(
            id UNINDEXED, title, abstract,
            content=publications, content_rowid=rowid,
            tokenize="unicode61 tokenchars '-'"
        )
    """)
    conn.execute("""
        INSERT INTO publications_fts(rowid, id, title, abstract)
        SELECT rowid, id, title, abstract FROM publications
    """)
    conn.commit()
    print("  FTS index rebuilt")


def main():
    parser = argparse.ArgumentParser(description="Update Flintstone database")
    parser.add_argument("--full", action="store_true", help="Full re-fetch (not incremental)")
    parser.add_argument("--since", type=str, help="Fetch works updated since this date (YYYY-MM-DD)")
    parser.add_argument("--skip-embeddings", action="store_true", help="Skip embedding generation")
    parser.add_argument("--skip-staff", action="store_true", help="Skip IGB staff directory refresh")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    if args.full:
        print("=== FULL REFRESH ===")
        # Run the original full fetch
        sys.path.insert(0, SCRIPT_DIR)
        from fetch_openalex import init_db, fetch_all_works, store_works as store_all
        init_db(conn)
        works = fetch_all_works()
        print(f"Fetched {len(works)} works")
        store_all(conn, works)
    else:
        since = args.since or get_last_update_date()
        print(f"=== INCREMENTAL UPDATE (since {since}) ===")
        works = fetch_works_since(since)
        if works:
            new_count, updated_count = store_works(conn, works)
            print(f"  {new_count} new, {updated_count} updated publications")
        else:
            print("  No new or updated works found")

    # Save timestamp
    save_last_update_date()

    if not args.skip_embeddings:
        print("\nUpdating embeddings...")
        new_embedded = rebuild_embeddings_incremental(conn)

    print("\nUpdating FTS index...")
    rebuild_fts(conn)

    if not args.skip_staff:
        print("\nRefreshing IGB staff directory...")
        try:
            sys.path.insert(0, SCRIPT_DIR)
            from fetch_igb_staff import fetch_all_staff, match_staff_to_authors
            staff = fetch_all_staff()
            matched, total = match_staff_to_authors(conn, staff)
            print(f"  Matched {matched} of {total} authors to current staff")
        except Exception as e:
            print(f"  Staff refresh failed (non-critical): {e}")

    # Summary
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM publications")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM publications WHERE abstract != ''")
    with_abs = cur.fetchone()[0]
    print(f"\n=== Database: {total} publications ({with_abs} with abstracts) ===")
    print(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    conn.close()
    print("\nDone! Restart the server to pick up changes.")


if __name__ == "__main__":
    main()
