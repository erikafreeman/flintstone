# Feuerstein: How I Built This

*An IGB Publication Intelligence Tool — built March 2026*

---

## What It Does

Feuerstein lets a researcher paste in a project abstract or research idea and instantly get:

1. **Relevant IGB publications** ranked by semantic similarity
2. **Potential collaborators** — IGB authors who have published on related topics
3. **Novelty assessment** — what's genuinely new about the idea vs. existing IGB work

The name is a nod to IGB's **FRED** data repository (like Fred Flintstone).

---

## How It Works

### Data Source: OpenAlex

[OpenAlex](https://openalex.org) is a free, open bibliographic database that indexes scientific publications worldwide. IGB is registered as institution `I4210116314`.

**What we pulled:**
- 6,743 IGB-affiliated publications (all time, 1970–2026)
- 4,730 of these have abstracts
- 1,690 unique IGB-affiliated authors
- For each publication: title, abstract, authors, journal, DOI, citation count, and topic tags

**How abstracts are stored:** OpenAlex returns abstracts as "inverted indices" — a compressed format where each word maps to its position(s) in the text. We reconstruct the full abstract by inverting this mapping.

### Semantic Search: Embeddings + Cosine Similarity

To find publications related to a query, we use **semantic search** — not keyword matching, but meaning-based matching.

**Step 1: Embed every publication**
- We use the `all-MiniLM-L6-v2` model (a small but effective sentence embedding model)
- Each publication's title + abstract is converted into a 384-dimensional vector
- These vectors capture the *meaning* of the text, not just the words
- We run the model locally using ONNX Runtime (no GPU or paid API needed)

**Step 2: Search by similarity**
- When you paste a query, it gets embedded using the same model
- We compute cosine similarity between the query vector and all 4,730 publication vectors
- The top results are returned, ranked by similarity score (shown as a percentage)

**Step 3: Post-processing**
- Duplicate versions of the same paper (preprint, Zenodo, published) are merged — we keep the one with the most citations
- Peer review artifacts ("Reply on RC1", "Comment on bg-2021-340") are filtered out
- Optional year range filters are applied

### Collaborator Ranking

For the top-k results, we count how often each IGB-affiliated author appears. Authors who show up in many of the relevant papers are likely good collaborators for your topic. We show their top 3 relevant papers.

### Novelty Detection (TF-IDF)

We use TF-IDF (Term Frequency–Inverse Document Frequency) to figure out which terms in your query are:

- **Well-covered**: present in many of the top matching IGB publications
- **Novel**: important to your query but rare or absent in IGB's existing work

The TF-IDF score gives higher weight to terms that are frequent in your query but rare in the matched corpus — these are likely the novel aspects of your idea.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3.12, FastAPI |
| Frontend | HTML, CSS, Jinja2 templates |
| Database | SQLite (flinstone.db) |
| Embeddings | all-MiniLM-L6-v2 via ONNX Runtime |
| Data source | OpenAlex API |
| Sharing | Cloudflare Tunnel (temporary links) |

**No paid APIs, no GPU, no cloud services required.** Everything runs on a single laptop.

---

## File Structure

```
flinstone/
  scripts/
    fetch_openalex.py      # Pull all IGB publications from OpenAlex
    build_embeddings.py     # Generate 384-dim vectors for each publication
  app/
    main.py                 # FastAPI routes (search, author profiles)
    search.py               # ONNX-based semantic search + dedup + filtering
    analysis.py             # Collaborator ranking + TF-IDF novelty detection
    models.py               # SQLite data access layer
    templates/
      index.html            # Main search page
      author.html           # Author profile page
    static/
      style.css             # IGB blue/green themed styles
  data/
    flinstone.db            # SQLite database (6,743 publications)
    embeddings.npy          # 4,730 x 384 embedding matrix
    pub_ids.npy             # Publication ID mapping for embeddings
    model/                  # ONNX model files (all-MiniLM-L6-v2)
```

---

## How to Run

```bash
# First time: install dependencies
pip install -r requirements.txt

# Refresh data from OpenAlex (optional — already done)
python scripts/fetch_openalex.py
python scripts/build_embeddings.py

# Start the app
cd flinstone
uvicorn app.main:app --port 8000

# Share temporarily via Cloudflare Tunnel
cloudflared tunnel --url http://localhost:8000
```

---

## Key Design Decisions

1. **ONNX Runtime instead of PyTorch** — sentence-transformers requires PyTorch + transformers + safetensors, which had build issues on Windows ARM64. ONNX Runtime runs the same model with no compilation needed.

2. **SQLite instead of PostgreSQL** — simple, zero-config, file-based. Perfect for a prototype with <10k records.

3. **Cosine similarity with numpy** — at 4,730 documents, brute-force cosine similarity takes <50ms. No need for approximate nearest-neighbor indexes (FAISS) until the corpus grows significantly.

4. **Deduplication by title** — OpenAlex indexes preprints, published versions, and data deposits separately. We merge these at query time, keeping the most-cited version.

5. **TF-IDF for novelty** — simple, interpretable, no black box. Researchers can see exactly which terms the system considers novel and why.
