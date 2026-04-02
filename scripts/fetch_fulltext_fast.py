"""
Fast full-text fetcher — targets domains known to serve PDFs directly.
Skips slow Unpaywall/CORE lookups. Much faster than fetch_fulltext.py.

Usage:
    python -m scripts.fetch_fulltext_fast
    python -m scripts.fetch_fulltext_fast --limit 200
"""

import sqlite3
import json
import os
import re
import sys
import time
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

CHUNK_SIZE = 400
CHUNK_OVERLAP = 80
MIN_CHUNK_WORDS = 50
USER_AGENT = "Feuerstein/1.0 (IGB Publication Intelligence; mailto:feuerstein@igb-berlin.de)"


def pdf_url_for(oa_url):
    """Convert an OA URL to a direct PDF URL based on known publisher patterns."""
    url = oa_url.strip()

    # Already a PDF
    if '.pdf' in url.lower().split('?')[0]:
        return url

    # Nature / Springer / BMC
    if 'nature.com/articles/' in url:
        return url.rstrip('/') + '.pdf'
    if 'link.springer.com/article/' in url:
        doi_match = re.search(r'10\.\d{4,}/[^\s?#]+', url)
        if doi_match:
            return f"https://link.springer.com/content/pdf/{doi_match.group()}.pdf"
    if 'biomedcentral.com/' in url:
        return url.replace('/articles/', '/counter/pdf/') if '/articles/' in url else url + '.pdf'

    # PLOS
    if 'journals.plos.org' in url and '/article?' in url:
        return url.replace('/article?', '/article/file?') + '&type=printable'

    # Frontiers
    if 'frontiersin.org/articles/' in url:
        return url.rstrip('/') + '/pdf'

    # MDPI
    if 'www.mdpi.com/' in url:
        return url.rstrip('/') + '/pdf'

    # bioRxiv / medRxiv
    if 'biorxiv.org/content/' in url or 'medrxiv.org/content/' in url:
        return url.rstrip('/') + '.full.pdf'

    # Copernicus (HESS, BG, ESSD, etc.)
    if 'copernicus.org/' in url:
        # e.g. https://hess.copernicus.org/articles/25/4867/2021/ -> add hess-25-4867-2021.pdf
        return url.rstrip('/') + '/' if not url.endswith('/') else url

    # Royal Society
    if 'royalsocietypublishing.org/doi/' in url:
        return url.replace('/doi/', '/doi/pdf/')

    # OUP (Oxford)
    if 'academic.oup.com/' in url:
        return url + '?redirectedFrom=PDF'

    # Elsevier direct
    if 'ars.els-cdn.com' in url:
        return url

    # Research Square preprints
    if 'researchsquare.com/' in url:
        return url + '.pdf' if '.pdf' not in url else url

    # IOP Science
    if 'iopscience.iop.org/' in url:
        return url + '/pdf' if '/pdf' not in url else url

    # PNAS
    if 'pnas.org/' in url:
        doi_match = re.search(r'10\.\d{4,}/[^\s?#]+', url)
        if doi_match:
            return url.replace('/abs/', '/pdf/').replace('/full/', '/pdf/')

    # Fallback: try appending .pdf
    return url.rstrip('/') + '.pdf'


def download_pdf(url, dest_path):
    """Download PDF with a single attempt (fast mode)."""
    if dest_path.exists() and dest_path.stat().st_size > 1000:
        return True

    pdf_url = pdf_url_for(url)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/pdf,*/*"}

    try:
        req = urllib.request.Request(pdf_url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read()
            if data[:5] == b'%PDF-':
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(data)
                return True
    except Exception:
        pass

    # One retry with the original URL if different
    if pdf_url != url:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = resp.read()
                if data[:5] == b'%PDF-':
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    dest_path.write_bytes(data)
                    return True
        except Exception:
            pass

    return False


def extract_text(pdf_path):
    from pypdf import PdfReader
    try:
        reader = PdfReader(str(pdf_path))
        texts = []
        for page in reader.pages:
            t = page.extract_text()
            if t:
                texts.append(t)
        return "\n\n".join(texts)
    except Exception:
        return None


def clean_text(text):
    if not text:
        return ""
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'\n\s*\d{1,3}\s*\n', '\n', text)
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def chunk_text(text, pub_id):
    # Strip references
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
        cw = words[start:end]
        if len(cw) >= MIN_CHUNK_WORDS:
            chunks.append((pub_id, idx, " ".join(cw)))
            idx += 1
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks


def embed_all_chunks(db):
    from tokenizers import Tokenizer
    import onnxruntime as ort

    model_path = MODEL_DIR / "onnx" / "model.onnx"
    tok_path = MODEL_DIR / "tokenizer.json"
    session = ort.InferenceSession(str(model_path))
    tokenizer = Tokenizer.from_file(str(tok_path))
    tokenizer.enable_truncation(max_length=384)
    tokenizer.enable_padding(pad_id=0, pad_token="[PAD]", length=384)
    input_names = [inp.name for inp in session.get_inputs()]

    all_chunks = db.execute("SELECT id, chunk_text FROM chunks ORDER BY id").fetchall()
    print(f"Embedding {len(all_chunks)} chunks...")

    BATCH = 64
    all_emb = []
    all_ids = []
    for i in range(0, len(all_chunks), BATCH):
        batch = all_chunks[i:i+BATCH]
        texts = [t for _, t in batch]
        ids = [cid for cid, _ in batch]

        encoded = tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
        attn = np.array([e.attention_mask for e in encoded], dtype=np.int64)

        feed = {"input_ids": input_ids, "attention_mask": attn}
        if "token_type_ids" in input_names:
            feed["token_type_ids"] = np.zeros_like(input_ids)

        out = session.run(None, feed)
        mask_exp = attn[:, :, np.newaxis].astype(np.float32)
        emb = np.sum(out[0] * mask_exp, axis=1) / np.clip(mask_exp.sum(axis=1), 1e-9, None)
        norms = np.linalg.norm(emb, axis=1, keepdims=True)
        emb = emb / np.clip(norms, 1e-9, None)

        all_emb.append(emb)
        all_ids.extend(ids)

        if (i // BATCH) % 20 == 0 and i > 0:
            print(f"  {i+len(batch)}/{len(all_chunks)}...")

    emb_arr = np.vstack(all_emb).astype(np.float32)
    np.save(str(BASE / "data" / "chunk_embeddings.npy"), emb_arr)
    np.save(str(BASE / "data" / "chunk_ids.npy"), np.array(all_ids))
    print(f"Saved {emb_arr.shape[0]} chunk embeddings ({emb_arr.shape[1]}-dim)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=99999)
    parser.add_argument("--embed-only", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(str(DB_PATH))
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    if not args.embed_only:
        # Get remaining OA papers from known-good domains
        GOOD_PATTERNS = [
            'nature.com', 'springer.com', 'plos.org', 'frontiersin.org',
            'mdpi.com', 'biorxiv.org', 'biomedcentral.com', 'iopscience.iop.org',
            'royalsocietypublishing.org', 'copernicus.org', 'ars.els-cdn.com',
            'researchsquare.com', 'pnas.org', 'int-res.com', '.pdf',
            'academic.oup.com', 'hal.science', 'medrxiv.org',
        ]

        remaining = db.execute('''
            SELECT p.id, p.oa_url FROM publications p
            WHERE p.oa_url IS NOT NULL AND p.oa_url != ''
            AND p.id NOT IN (SELECT DISTINCT publication_id FROM chunks)
            ORDER BY p.cited_by_count DESC
        ''').fetchall()

        targets = []
        for pub_id, url in remaining:
            if any(pat in url.lower() for pat in GOOD_PATTERNS):
                targets.append((pub_id, url))

        targets = targets[:args.limit]
        print(f"Targeting {len(targets)} papers from reliable PDF sources...")

        success = 0
        failed = 0
        total_chunks = 0

        for i, (pub_id, url) in enumerate(targets):
            safe_id = pub_id.replace("https://openalex.org/", "")
            pdf_path = PDF_DIR / f"{safe_id}.pdf"

            if download_pdf(url, pdf_path):
                raw = extract_text(pdf_path)
                if raw and len(raw) > 500:
                    text = clean_text(raw)
                    chunks = chunk_text(text, pub_id)
                    if chunks:
                        for pid, idx, ct in chunks:
                            try:
                                db.execute("INSERT OR IGNORE INTO chunks (publication_id, chunk_index, chunk_text) VALUES (?,?,?)",
                                           (pid, idx, ct))
                            except:
                                pass
                        db.commit()
                        total_chunks += len(chunks)
                        success += 1

                # Clean up PDF
                if pdf_path.exists():
                    pdf_path.unlink()
            else:
                failed += 1

            if (i + 1) % 50 == 0:
                print(f"  {i+1}/{len(targets)} | ok={success} fail={failed} chunks={total_chunks}")

            time.sleep(0.3)

        print(f"\nDone: {success} papers -> {total_chunks} chunks (failed: {failed})")

    # Embed
    chunk_count = db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    if chunk_count > 0:
        embed_all_chunks(db)

    db.close()


if __name__ == "__main__":
    main()
