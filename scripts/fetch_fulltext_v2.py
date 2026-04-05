"""
Improved full-text pipeline for Feuerstein — V2.

Handles the main failure modes from V1:
1. HTML scraping for papers where PDF download fails (bioRxiv, MDPI, OUP, PLoS, etc.)
2. Smarter Unpaywall: tries ALL OA locations, not just best
3. Green OA repository landing pages (hdl.handle.net, zenodo, hal, etc.)
4. DOI-based redirects (doi.org → publisher → PDF)
5. Requests library with proper redirects (replaces urllib)

Usage:
    python scripts/fetch_fulltext_v2.py              # process failed papers
    python scripts/fetch_fulltext_v2.py --limit 100  # test with 100
    python scripts/fetch_fulltext_v2.py --embed-only  # skip download, just embed
"""

import sqlite3
import json
import os
import re
import sys
import time
import hashlib
import argparse
import numpy as np
from pathlib import Path

import requests as req_lib
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding="utf-8")

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "flinstone.db"
PDF_DIR = BASE / "data" / "pdfs"
MODEL_DIR = BASE / "data" / "model_specter2"

CHUNK_SIZE = 400
CHUNK_OVERLAP = 80
MIN_CHUNK_WORDS = 50
RATE_LIMIT_DELAY = 0.4

SESSION = req_lib.Session()
SESSION.headers.update({
    "User-Agent": "Feuerstein/2.0 (IGB Publication Intelligence; mailto:feuerstein@igb-berlin.de)",
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
})
SESSION.max_redirects = 10


# ── Text extraction strategies ──────────────────────────────────────

def try_pdf_download(url):
    """Try to download and extract text from a PDF URL."""
    try:
        resp = SESSION.get(url, timeout=30, allow_redirects=True)
        if resp.status_code == 200 and resp.content[:5] == b'%PDF-':
            return extract_text_from_pdf_bytes(resp.content)
    except Exception:
        pass
    return None


def extract_text_from_pdf_bytes(data):
    """Extract text from PDF bytes using pypdf."""
    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(data))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        full = "\n\n".join(pages)
        return full if len(full) > 500 else None
    except Exception:
        return None


def scrape_html_fulltext(url):
    """Scrape full text from an HTML article page."""
    try:
        resp = SESSION.get(url, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script, style, nav, footer, header elements
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Try known article body selectors (most common publishers)
        selectors = [
            # bioRxiv / medRxiv
            "div.article__body",
            "div.article-fulltext",
            # MDPI
            "div.html-body",
            "article div.html-p",
            # OUP (Oxford)
            "div.article-body",
            "section.article-body-section",
            # PLoS
            "div.article-text",
            "div#artText",
            # Springer / Nature
            "div.c-article-body",
            "article div.body",
            "div#body",
            # Wiley
            "div.article__body",
            "section.article-section__full",
            # Frontiers
            "div.JournalFullText",
            # Taylor & Francis
            "div.NLM_sec_level_1",
            # Cell / Elsevier
            "div.body-content",
            "div#abstracts + div",
            # Generic fallbacks
            "article",
            "main",
            "div.content",
            "div.fulltext",
            "div#content",
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                text_parts = []
                for el in elements:
                    text = el.get_text(separator=" ", strip=True)
                    if len(text) > 200:
                        text_parts.append(text)
                if text_parts:
                    full = "\n\n".join(text_parts)
                    if len(full) > 500:
                        return full

        # Last resort: get all <p> tags in the page
        paragraphs = soup.find_all("p")
        if len(paragraphs) > 10:
            full = "\n\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50)
            if len(full) > 1000:
                return full

        return None
    except Exception:
        return None


def try_biorxiv_medrxiv(doi):
    """bioRxiv/medRxiv have reliable full-text HTML and PDF."""
    if not doi:
        return None

    # Try bioRxiv HTML first (more reliable than PDF)
    for server in ["biorxiv", "medrxiv"]:
        html_url = f"https://www.{server}.org/content/{doi}.full"
        text = scrape_html_fulltext(html_url)
        if text:
            return text

        # Try PDF
        pdf_url = f"https://www.{server}.org/content/{doi}.full.pdf"
        text = try_pdf_download(pdf_url)
        if text:
            return text

    return None


def try_unpaywall_all_locations(doi):
    """Try ALL OA locations from Unpaywall, not just the 'best' one."""
    if not doi:
        return None

    try:
        resp = SESSION.get(
            f"https://api.unpaywall.org/v2/{doi}",
            params={"email": "feuerstein@igb-berlin.de"},
            timeout=15,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        locations = data.get("oa_locations") or []

        for loc in locations:
            # Try PDF URL first
            pdf_url = loc.get("url_for_pdf")
            if pdf_url:
                text = try_pdf_download(pdf_url)
                if text:
                    return text

            # Try landing page HTML
            landing = loc.get("url_for_landing_page") or loc.get("url")
            if landing:
                text = scrape_html_fulltext(landing)
                if text:
                    return text

    except Exception:
        pass
    return None


def try_repository_landing_page(url):
    """Handle repository landing pages (hdl.handle.net, zenodo, hal, etc.)."""
    try:
        # Follow redirects to the actual page
        resp = SESSION.get(url, timeout=20, allow_redirects=True)
        if resp.status_code != 200:
            return None

        final_url = resp.url
        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for PDF download links on the landing page
        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = (a.get_text() or "").lower()
            if ".pdf" in href.lower() or "download" in text or "full text" in text or "fulltext" in text:
                if href.startswith("/"):
                    from urllib.parse import urljoin
                    href = urljoin(final_url, href)
                pdf_links.append(href)

        # Try each PDF link
        for pdf_url in pdf_links[:3]:  # Max 3 attempts
            text = try_pdf_download(pdf_url)
            if text:
                return text

        # If no PDF links found, try scraping the page itself
        text = scrape_html_fulltext(final_url)
        if text:
            return text

    except Exception:
        pass
    return None


def try_doi_redirect(doi):
    """Follow DOI redirect to publisher, then try HTML scraping."""
    if not doi:
        return None

    try:
        doi_url = f"https://doi.org/{doi}" if not doi.startswith("http") else doi
        resp = SESSION.get(doi_url, timeout=20, allow_redirects=True)
        if resp.status_code != 200:
            return None

        final_url = resp.url

        # Try PDF variants of the final URL
        pdf_variants = [final_url]
        if ".pdf" not in final_url:
            pdf_variants.append(final_url.rstrip("/") + ".pdf")
            pdf_variants.append(final_url.rstrip("/") + "/pdf")

        # Publisher-specific PDF URLs
        if "wiley.com" in final_url:
            pdf_variants.append(final_url.replace("/abs/", "/pdf/").replace("/full/", "/pdf/"))
            if "/doi/" in final_url and "pdfdirect" not in final_url:
                pdf_variants.append(final_url.replace("/doi/", "/doi/pdfdirect/"))
        elif "springer.com" in final_url or "nature.com" in final_url:
            doi_match = re.search(r'10\.\d{4,}/[^\s?#]+', final_url)
            if doi_match:
                pdf_variants.append(f"https://link.springer.com/content/pdf/{doi_match.group()}.pdf")
        elif "oup.com" in final_url:
            # OUP: try article page with ?login=false for full HTML
            if "?" not in final_url:
                pdf_variants.append(final_url + "?login=false")
        elif "pnas.org" in final_url or "sciencemag.org" in final_url:
            pdf_variants.append(final_url + ".full.pdf")

        # Try PDF downloads
        for pdf_url in pdf_variants[1:]:  # Skip the HTML page itself
            text = try_pdf_download(pdf_url)
            if text:
                return text

        # Fall back to HTML scraping of the publisher page
        text = scrape_html_fulltext(final_url)
        if text:
            return text

    except Exception:
        pass
    return None


def try_europepmc(doi):
    """Europe PMC often has free full text for biomedical/ecology papers."""
    if not doi:
        return None

    try:
        # Search by DOI
        resp = SESSION.get(
            "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
            params={"query": f'DOI:"{doi}"', "format": "json", "resultType": "core"},
            timeout=15,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        results = data.get("resultList", {}).get("result", [])
        if not results:
            return None

        result = results[0]
        pmcid = result.get("pmcid")

        if pmcid:
            # Try full text XML from Europe PMC
            xml_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
            xml_resp = SESSION.get(xml_url, timeout=15)
            if xml_resp.status_code == 200:
                soup = BeautifulSoup(xml_resp.text, "xml")
                body = soup.find("body")
                if body:
                    text = body.get_text(separator=" ", strip=True)
                    if len(text) > 500:
                        return text

    except Exception:
        pass
    return None


# ── Main pipeline ───────────────────────────────────────────────────

def get_text_for_paper(oa_url, doi, oa_type):
    """Try all strategies to get full text for a paper."""
    clean_doi = None
    if doi:
        doi_match = re.search(r'(10\.\d{4,}/[^\s?#]+)', doi)
        if doi_match:
            clean_doi = doi_match.group(1)

    # Strategy 1: bioRxiv/medRxiv (very reliable)
    if clean_doi and ("biorxiv" in (oa_url or "") or "medrxiv" in (oa_url or "")):
        text = try_biorxiv_medrxiv(clean_doi)
        if text:
            return text, "biorxiv"

    # Strategy 2: Direct PDF download from OA URL
    if oa_url:
        text = try_pdf_download(oa_url)
        if text:
            return text, "pdf_direct"

    # Strategy 3: Repository landing pages
    if oa_url and any(domain in oa_url for domain in [
        "hdl.handle.net", "zenodo.org", "hal.science", "hal.archives",
        "repository.", "research.", "dspace.", "opus.", "publisso"
    ]):
        text = try_repository_landing_page(oa_url)
        if text:
            return text, "repository"

    # Strategy 4: Europe PMC (free full text for many papers)
    if clean_doi:
        text = try_europepmc(clean_doi)
        if text:
            return text, "europepmc"

    # Strategy 5: Unpaywall — try ALL locations
    if clean_doi:
        text = try_unpaywall_all_locations(clean_doi)
        if text:
            return text, "unpaywall"

    # Strategy 6: Follow DOI redirect → publisher → scrape HTML
    if clean_doi:
        text = try_doi_redirect(clean_doi)
        if text:
            return text, "doi_scrape"

    # Strategy 7: Direct HTML scrape of the OA URL
    if oa_url and oa_url != doi:
        text = scrape_html_fulltext(oa_url)
        if text:
            return text, "html_direct"

    return None, None


def clean_text(text):
    """Clean extracted text."""
    if not text:
        return ""
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'\n\s*\d{1,3}\s*\n', '\n', text)
    text = re.sub(r'(?m)^.*Downloaded from.*$', '', text)
    text = re.sub(r'(?m)^.*Copyright \d{4}.*$', '', text)
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n ', '\n', text)
    return text.strip()


def chunk_text(text, pub_id):
    """Split text into overlapping chunks."""
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
            chunk_text_str = " ".join(chunk_words)

            # Detect section
            lower = chunk_text_str[:200].lower()
            section = None
            for name, pattern in [
                ('abstract', r'\babstract\b'),
                ('introduction', r'\bintroduction\b'),
                ('methods', r'\b(?:methods?|materials?\s+and\s+methods?)\b'),
                ('results', r'\bresults?\b'),
                ('discussion', r'\bdiscussion\b'),
                ('conclusion', r'\bconclusion'),
                ('references', r'\breferences\b'),
            ]:
                if re.search(pattern, lower):
                    section = name
                    break

            if section == 'references':
                break

            chunks.append({
                "publication_id": pub_id,
                "chunk_index": idx,
                "chunk_text": chunk_text_str,
                "section": section,
            })
            idx += 1

        start += CHUNK_SIZE - CHUNK_OVERLAP

    return chunks


def embed_chunks(db):
    """Embed all new chunks using ONNX model."""
    from tokenizers import Tokenizer
    import onnxruntime as ort

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

    all_chunks = db.execute("SELECT id, chunk_text FROM chunks ORDER BY id").fetchall()

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

    BATCH_SIZE = 32
    all_embeddings = []
    all_ids = []

    for i in range(0, len(new_chunks), BATCH_SIZE):
        batch = new_chunks[i:i + BATCH_SIZE]
        texts = [text for _, text in batch]
        ids = [cid for cid, _ in batch]

        encoded = tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
        attention_mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
        token_type_ids = np.zeros_like(input_ids)

        input_names = [inp.name for inp in session.get_inputs()]
        feed = {"input_ids": input_ids, "attention_mask": attention_mask}
        if "token_type_ids" in input_names:
            feed["token_type_ids"] = token_type_ids

        outputs = session.run(None, feed)

        token_embeddings = outputs[0]
        mask_expanded = attention_mask[:, :, np.newaxis].astype(np.float32)
        summed = np.sum(token_embeddings * mask_expanded, axis=1)
        counts = np.clip(mask_expanded.sum(axis=1), 1e-9, None)
        embeddings = summed / counts

        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        embeddings = embeddings / np.clip(norms, 1e-9, None)

        all_embeddings.append(embeddings)
        all_ids.extend(ids)

        if (i // BATCH_SIZE) % 50 == 0:
            print(f"  Embedded {i + len(batch)}/{len(new_chunks)} chunks...")

    new_embeddings = np.vstack(all_embeddings).astype(np.float32)

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

    print(f"Saved {len(all_emb)} chunk embeddings ({all_emb.shape[1]}-dim)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Limit number of papers to process")
    parser.add_argument("--embed-only", action="store_true", help="Skip download, just embed")
    args = parser.parse_args()

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

    if not args.embed_only:
        # Get papers that have OA URLs but no chunks yet
        sql = """
            SELECT p.id, p.title, p.doi, p.oa_url, p.oa_type
            FROM publications p
            WHERE p.oa_url IS NOT NULL AND p.oa_url != ''
              AND p.id NOT IN (SELECT DISTINCT publication_id FROM chunks)
            ORDER BY p.cited_by_count DESC
        """
        if args.limit:
            sql += f" LIMIT {args.limit}"
        pubs = db.execute(sql).fetchall()
        print(f"Found {len(pubs)} papers to retry with improved strategies")

        success = 0
        failed = 0
        skipped = 0
        total_chunks = 0
        by_strategy = {}

        for i, (pub_id, title, doi, oa_url, oa_type) in enumerate(pubs):
            text, strategy = get_text_for_paper(oa_url, doi, oa_type)

            if text and len(text) > 500:
                text = clean_text(text)
                chunks = chunk_text(text, pub_id)

                if chunks:
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
                    by_strategy[strategy] = by_strategy.get(strategy, 0) + 1
                else:
                    skipped += 1
            else:
                failed += 1

            if (i + 1) % 25 == 0:
                print(f"  Progress: {i+1}/{len(pubs)} | OK {success} | fail {failed} | skip {skipped} | chunks {total_chunks}")

            time.sleep(RATE_LIMIT_DELAY)

        print(f"\n=== Download complete ===")
        print(f"  Extracted: {success} papers -> {total_chunks} chunks")
        print(f"  Failed: {failed}")
        print(f"  Skipped: {skipped}")
        print(f"\n  By strategy:")
        for strat, count in sorted(by_strategy.items(), key=lambda x: -x[1]):
            print(f"    {strat:20s} {count}")

    # Phase 2: Embed
    chunk_count = db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    if chunk_count > 0:
        print(f"\nPhase 2: Embedding {chunk_count} chunks...")
        embed_chunks(db)
    else:
        print("No chunks to embed.")

    db.close()


if __name__ == "__main__":
    main()
