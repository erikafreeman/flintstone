"""
Full-text pipeline for Feuerstein.

1. Downloads OA PDFs (respecting rate limits)
2. Extracts text with pypdf
3. Chunks into ~400-token overlapping segments
4. Embeds each chunk with all-mpnet-base-v2 (ONNX)
5. Stores in SQLite + numpy for search

Usage:
    python -m scripts.fetch_fulltext          # process all OA papers
    python -m scripts.fetch_fulltext --limit 100  # test with 100 papers
    python -m scripts.fetch_fulltext --embed-only  # skip download, just re-embed
"""

import sqlite3
import json
import os
import re
import sys
import time
import hashlib
import argparse
import urllib.request
import urllib.error
import numpy as np
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "flinstone.db"
PDF_DIR = BASE / "data" / "pdfs"
MODEL_DIR = BASE / "data" / "model_specter2"

# ── Chunking parameters ─────────────────────────────────────────────
CHUNK_SIZE = 400       # tokens (words as proxy)
CHUNK_OVERLAP = 80     # overlap tokens
MIN_CHUNK_WORDS = 50   # skip tiny chunks

# ── Download parameters ──────────────────────────────────────────────
MAX_RETRIES = 2
TIMEOUT = 30
RATE_LIMIT_DELAY = 0.5  # seconds between downloads
USER_AGENT = "Feuerstein/1.0 (IGB Publication Intelligence; mailto:feuerstein@igb-berlin.de)"


def setup_db():
    """Create chunks table if it doesn't exist."""
    db = sqlite3.connect(str(DB_PATH))
    db.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publication_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_text TEXT NOT NULL,
            section TEXT,
            UNIQUE(publication_id, chunk_index)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_chunks_pub ON chunks(publication_id)")
    db.commit()
    return db


def get_oa_publications(db, limit=None):
    """Get publications with OA URLs that haven't been chunked yet."""
    sql = """
        SELECT p.id, p.title, p.doi, p.oa_url, p.abstract
        FROM publications p
        WHERE p.oa_url IS NOT NULL AND p.oa_url != ''
          AND p.id NOT IN (SELECT DISTINCT publication_id FROM chunks)
        ORDER BY p.cited_by_count DESC
    """
    if limit:
        sql += f" LIMIT {limit}"
    return db.execute(sql).fetchall()


def _try_download(url, dest_path, headers):
    """Try to download a PDF from a single URL. Returns True on success."""
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            data = resp.read()
            if data[:5] == b'%PDF-':
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(data)
                return True
    except Exception:
        pass
    return False


def download_pdf(url, dest_path):
    """Download a PDF from URL. Tries multiple strategies. Returns True on success."""
    if dest_path.exists() and dest_path.stat().st_size > 1000:
        return True

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/pdf,*/*",
    }

    # Strategy 1: Try the URL directly
    if _try_download(url, dest_path, headers):
        return True

    # Strategy 2: If URL has .pdf, already tried. Otherwise try appending .pdf
    if '.pdf' not in url:
        pdf_url = url.rstrip('/') + '.pdf'
        if _try_download(pdf_url, dest_path, headers):
            return True

    # Strategy 3: For Wiley, convert to pdfdirect
    if 'wiley.com/doi/' in url and 'pdfdirect' not in url:
        wiley_url = url.replace('/doi/', '/doi/pdfdirect/')
        if _try_download(wiley_url, dest_path, headers):
            return True

    # Strategy 4: For Springer/Nature, try /content/pdf/DOI.pdf
    if 'nature.com/articles/' in url or 'springer.com/article/' in url:
        doi_match = re.search(r'10\.\d{4,}/[^\s]+', url)
        if doi_match:
            springer_url = f"https://link.springer.com/content/pdf/{doi_match.group()}.pdf"
            if _try_download(springer_url, dest_path, headers):
                return True

    # Strategy 5: For DOI URLs, try Unpaywall
    doi_match = re.search(r'10\.\d{4,}/[^\s?#]+', url)
    if doi_match:
        doi = doi_match.group()
        unpaywall_url = f"https://api.unpaywall.org/v2/{doi}?email=feuerstein@igb-berlin.de"
        try:
            req = urllib.request.Request(unpaywall_url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                best_loc = data.get("best_oa_location", {})
                pdf_url = best_loc.get("url_for_pdf")
                if pdf_url and _try_download(pdf_url, dest_path, headers):
                    return True
        except Exception:
            pass

    # Strategy 6: Try CORE API (large open access aggregator)
    if doi_match:
        doi = doi_match.group()
        core_url = f"https://core.ac.uk/api-v2/articles/doi/{doi}"
        try:
            req = urllib.request.Request(core_url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                dl_url = data.get("downloadUrl")
                if dl_url and _try_download(dl_url, dest_path, headers):
                    return True
        except Exception:
            pass

    return False


def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using pypdf."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(pdf_path))
        pages_text = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
        return "\n\n".join(pages_text)
    except Exception as e:
        return None


def clean_text(text):
    """Clean extracted PDF text."""
    if not text:
        return ""

    # Remove excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Remove page numbers (standalone numbers on a line)
    text = re.sub(r'\n\s*\d{1,3}\s*\n', '\n', text)
    # Remove common header/footer patterns
    text = re.sub(r'(?m)^.*Downloaded from.*$', '', text)
    text = re.sub(r'(?m)^.*Copyright.*$', '', text)
    # Fix hyphenated line breaks
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n ', '\n', text)

    return text.strip()


def detect_section(text_block):
    """Try to detect what section a chunk belongs to."""
    lower = text_block[:200].lower()
    sections = [
        ('abstract', r'\babstract\b'),
        ('introduction', r'\bintroduction\b'),
        ('methods', r'\b(?:methods?|materials?\s+and\s+methods?|study\s+(?:area|site))\b'),
        ('results', r'\bresults?\b'),
        ('discussion', r'\bdiscussion\b'),
        ('conclusion', r'\bconclusion'),
        ('references', r'\breferences\b'),
        ('acknowledgements', r'\backnowledg'),
        ('supplementary', r'\bsupplementary\b|\bappendix\b'),
    ]
    for name, pattern in sections:
        if re.search(pattern, lower):
            return name
    return None


def chunk_text(text, pub_id):
    """Split text into overlapping chunks of ~CHUNK_SIZE words."""
    # Skip references section
    ref_match = re.search(r'\n\s*References\s*\n', text, re.IGNORECASE)
    if ref_match:
        text = text[:ref_match.start()]

    words = text.split()
    if len(words) < MIN_CHUNK_WORDS:
        return []

    chunks = []
    start = 0
    idx = 0

    while start < len(words):
        end = min(start + CHUNK_SIZE, len(words))
        chunk_words = words[start:end]

        if len(chunk_words) >= MIN_CHUNK_WORDS:
            chunk_text = " ".join(chunk_words)
            section = detect_section(chunk_text)

            # Skip chunks that are mostly references/bibliography
            if section == 'references':
                break

            chunks.append({
                "publication_id": pub_id,
                "chunk_index": idx,
                "chunk_text": chunk_text,
                "section": section,
            })
            idx += 1

        start += CHUNK_SIZE - CHUNK_OVERLAP

    return chunks


def embed_chunks(db):
    """Embed all chunks using ONNX model."""
    from tokenizers import Tokenizer
    import onnxruntime as ort

    # Load model
    model_path = MODEL_DIR / "onnx" / "model.onnx"
    tok_path = MODEL_DIR / "tokenizer.json"

    if not model_path.exists():
        print("ERROR: ONNX model not found at", model_path)
        return

    print("Loading ONNX model for chunk embedding...")
    session = ort.InferenceSession(str(model_path))
    tokenizer = Tokenizer.from_file(str(tok_path))
    tokenizer.enable_truncation(max_length=384)
    tokenizer.enable_padding(pad_id=0, pad_token="[PAD]", length=384)

    # Get all chunks without embeddings
    all_chunks = db.execute("SELECT id, chunk_text FROM chunks ORDER BY id").fetchall()

    # Check if we already have a chunk embeddings file
    emb_path = BASE / "data" / "chunk_embeddings.npy"
    id_path = BASE / "data" / "chunk_ids.npy"

    if emb_path.exists() and id_path.exists():
        existing_ids = set(np.load(str(id_path)))
        new_chunks = [(cid, text) for cid, text in all_chunks if cid not in existing_ids]
        if not new_chunks:
            print(f"All {len(all_chunks)} chunks already embedded.")
            return
        print(f"{len(new_chunks)} new chunks to embed ({len(all_chunks) - len(new_chunks)} already done)")
    else:
        new_chunks = all_chunks
        print(f"Embedding {len(new_chunks)} chunks...")

    # Batch embed
    BATCH_SIZE = 32
    all_embeddings = []
    all_ids = []

    for i in range(0, len(new_chunks), BATCH_SIZE):
        batch = new_chunks[i:i + BATCH_SIZE]
        texts = [text for _, text in batch]
        ids = [cid for cid, _ in batch]

        # Tokenize
        encoded = tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
        attention_mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
        token_type_ids = np.zeros_like(input_ids)

        # Run inference (build input dict based on model's expected inputs)
        input_names = [inp.name for inp in session.get_inputs()]
        feed = {"input_ids": input_ids, "attention_mask": attention_mask}
        if "token_type_ids" in input_names:
            feed["token_type_ids"] = token_type_ids

        outputs = session.run(None, feed)

        # Mean pooling
        token_embeddings = outputs[0]  # (batch, seq, hidden)
        mask_expanded = attention_mask[:, :, np.newaxis].astype(np.float32)
        summed = np.sum(token_embeddings * mask_expanded, axis=1)
        counts = np.clip(mask_expanded.sum(axis=1), 1e-9, None)
        embeddings = summed / counts

        # Normalize
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        embeddings = embeddings / np.clip(norms, 1e-9, None)

        all_embeddings.append(embeddings)
        all_ids.extend(ids)

        if (i // BATCH_SIZE) % 50 == 0:
            print(f"  Embedded {i + len(batch)}/{len(new_chunks)} chunks...")

    new_embeddings = np.vstack(all_embeddings).astype(np.float32)

    # Merge with existing if any
    if emb_path.exists() and id_path.exists():
        old_emb = np.load(str(emb_path))
        old_ids = np.load(str(id_path))
        all_emb = np.vstack([old_emb, new_embeddings])
        all_id_arr = np.concatenate([old_ids, np.array(all_ids)])
    else:
        all_emb = new_embeddings
        all_id_arr = np.array(all_ids)

    np.save(str(emb_path), all_emb)
    np.save(str(id_path), all_id_arr)

    print(f"✓ Saved {len(all_emb)} chunk embeddings ({all_emb.shape[1]}-dim)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Limit number of papers to process")
    parser.add_argument("--embed-only", action="store_true", help="Skip download, just embed existing chunks")
    args = parser.parse_args()

    db = setup_db()
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    if not args.embed_only:
        # Phase 1: Download and extract
        pubs = get_oa_publications(db, args.limit)
        print(f"Found {len(pubs)} OA publications to process")

        success = 0
        failed = 0
        skipped = 0
        total_chunks = 0

        for i, (pub_id, title, doi, oa_url, abstract) in enumerate(pubs):
            # Create a filename from the publication ID
            safe_id = pub_id.replace("https://openalex.org/", "")
            pdf_path = PDF_DIR / f"{safe_id}.pdf"

            # Download
            if download_pdf(oa_url, pdf_path):
                # Extract text
                raw_text = extract_text_from_pdf(pdf_path)
                if raw_text and len(raw_text) > 500:
                    text = clean_text(raw_text)
                    chunks = chunk_text(text, pub_id)

                    if chunks:
                        # Store chunks
                        for chunk in chunks:
                            try:
                                db.execute(
                                    "INSERT OR IGNORE INTO chunks (publication_id, chunk_index, chunk_text, section) VALUES (?,?,?,?)",
                                    (chunk["publication_id"], chunk["chunk_index"], chunk["chunk_text"], chunk["section"])
                                )
                            except sqlite3.IntegrityError:
                                pass
                        db.commit()
                        total_chunks += len(chunks)
                        success += 1
                    else:
                        skipped += 1
                else:
                    skipped += 1

                # Clean up PDF to save disk space (keep only text in DB)
                if pdf_path.exists():
                    pdf_path.unlink()
            else:
                failed += 1

            if (i + 1) % 25 == 0:
                print(f"  Progress: {i+1}/{len(pubs)} | ✓ {success} | ✗ {failed} | skip {skipped} | chunks {total_chunks}")

            time.sleep(RATE_LIMIT_DELAY)

        print(f"\nDownload complete:")
        print(f"  ✓ Extracted: {success} papers → {total_chunks} chunks")
        print(f"  ✗ Failed: {failed}")
        print(f"  ⊘ Skipped: {skipped} (no usable text)")

    # Phase 2: Embed all chunks
    chunk_count = db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    if chunk_count > 0:
        print(f"\nPhase 2: Embedding {chunk_count} chunks...")
        embed_chunks(db)
    else:
        print("No chunks to embed.")

    db.close()


if __name__ == "__main__":
    main()
