# Feuerstein: How It Works

*A beginner-friendly guide to the IGB Publication Intelligence Tool*

---

## What is Feuerstein?

Feuerstein is a search engine and analytics dashboard built specifically for IGB (Leibniz Institute of Freshwater Ecology and Inland Fisheries). It helps researchers:

- **Find relevant prior work** by pasting a research idea or abstract
- **Identify potential collaborators** at IGB who work on similar topics
- **Assess novelty** of a research idea against IGB's publication record
- **Explore open science practices** across IGB's researchers
- **Map collaboration networks** with external institutions worldwide

The name "Feuerstein" is German for "flintstone" — a nod to IGB's FRED data repository (like Fred Flintstone).

---

## The Data: Where Does It Come From?

### OpenAlex

All publication data comes from [OpenAlex](https://openalex.org), a free, open bibliographic database that indexes over 250 million scientific works. IGB is registered as institution `I4210116314`.

**What we pulled:**
- **6,743 publications** affiliated with IGB (1970-2026)
- **4,730** of these have abstracts (70%)
- **18,705 unique authors** (including co-authors from other institutions)
- For each publication: title, abstract, authors, journal, DOI, citation count, open access status, and topic tags

**How abstracts work:** OpenAlex stores abstracts as "inverted indices" — a compressed format where each word maps to its position(s) in the text. For example, `{"water": [0, 5], "quality": [1]}` means "water" appears at positions 0 and 5, and "quality" at position 1. We reconstruct the original text by sorting words by position.

### IGB Staff Directory

We scrape the [IGB website](https://www.igb-berlin.de) to get current staff information:
- **309 current staff members** matched to their OpenAlex author profiles
- Department assignments (Dept. 1-6)
- Position titles (Group Leader, Postdoc, PhD Student, etc.)

This lets us show department badges and "current staff" indicators on search results.

### Full-Text Papers

For deeper search, we download open-access PDFs and extract their full text:
- **10,063 text chunks** from 296+ open-access papers
- Each paper is split into ~400-word chunks with 80-word overlap
- This means searches can match specific sections of papers, not just abstracts

---

## How Search Works

This is the core of Feuerstein. When you paste a research idea, it goes through a multi-stage pipeline:

### Stage 1: Query Expansion

Your query is first expanded with domain-specific knowledge. Feuerstein knows 59 abbreviations common in freshwater ecology:

| You type | Feuerstein adds |
|----------|----------------|
| DOM | dissolved organic matter |
| eDNA | environmental DNA |
| FT-ICR-MS | Fourier transform ion cyclotron resonance mass spectrometry |
| GHG | greenhouse gas |
| WFD | Water Framework Directive |

This helps match papers that use the full term when you typed the abbreviation (or vice versa).

### Stage 2: Semantic Search (Understanding Meaning)

This is where Feuerstein goes beyond simple keyword matching.

**What are embeddings?**

Imagine you could summarize the *meaning* of any text as a list of 768 numbers (a "vector"). Two texts about similar topics would have similar numbers. Two texts about different topics would have very different numbers.

That's what an embedding model does. We use **SPECTER2** (also called all-mpnet-base-v2), a model specifically trained on scientific papers. It converts text into a 768-dimensional vector that captures its meaning.

**How it works step by step:**

1. **Offline (done once):** Every publication's title + abstract is converted into a 768-number vector. We store all 4,730 vectors in a file (`embeddings_mpnet.npy`).

2. **At search time:** Your query is converted into the same kind of 768-number vector.

3. **Similarity:** We calculate the "cosine similarity" between your query vector and all 4,730 publication vectors. This measures how aligned two vectors are — 1.0 means identical direction (same topic), 0.0 means completely unrelated.

4. **Full-text chunks:** We also search against 10,063 text chunks from full papers. This catches relevant content buried deep in a paper that might not appear in the abstract.

**Why is this better than keyword search?**

- A keyword search for "warming" won't find a paper about "climate change effects on temperature." Semantic search will, because the meanings are similar.
- A keyword search for "fish" won't find a paper about "ichthyofauna." Semantic search understands they're related.

### Stage 3: Keyword Search (Belt and Suspenders)

We also run a traditional keyword search using SQLite's FTS5 (Full-Text Search). This catches exact matches that the semantic model might rank slightly lower.

### Stage 4: Reciprocal Rank Fusion (Combining Results)

Now we have three ranked lists:
1. Semantic search on abstracts
2. Semantic search on full-text chunks
3. Keyword search

We need to merge them. We use **Reciprocal Rank Fusion (RRF)**, a simple but effective formula:

```
score(paper) = sum over all lists of: 1 / (k + rank_in_list)
```

where `k = 60` (a smoothing constant). A paper ranked #1 in one list gets `1/61 = 0.016`. A paper ranked #1 in two lists gets `2 * 0.016 = 0.033`. This naturally surfaces papers that appear high across multiple search methods.

### Stage 5: Cross-Encoder Re-Ranking

The top ~50 candidates from RRF are re-scored using a **cross-encoder** — a more accurate (but slower) model that looks at the query and each document *together* instead of separately.

Think of it this way:
- **Embedding search** (Stage 2) is like summarizing two books and comparing the summaries. Fast, but loses nuance.
- **Cross-encoder** (Stage 5) is like reading both books side by side and judging how related they are. Slow, but much more accurate.

We use a 87MB cross-encoder model that scores query-document pairs. The top 20 results are returned to you.

### Stage 6: Post-Processing

Before showing results, we clean up:
- **Deduplication:** The same paper often appears multiple times in OpenAlex (preprint, published version, data deposit). We keep the version with the most citations.
- **Filter artifacts:** Peer review comments ("Reply on RC1", "Comment on bg-2021-340") are removed.
- **Year filtering:** If you set a year range, results outside it are dropped.

---

## Features Beyond Search

### Potential Collaborators

For your search results, Feuerstein counts how often each IGB author appears. Authors in many of the top papers are likely good collaborators for your topic. Current staff are flagged with a checkmark.

### Novelty Assessment (TF-IDF)

Feuerstein analyzes which parts of your idea are truly new versus well-covered at IGB.

**How?** Using TF-IDF (Term Frequency-Inverse Document Frequency):
- **High TF-IDF terms:** Words important to your query but rare in the matching publications. These are your novel contributions.
- **Low TF-IDF terms:** Words common in both your query and the results. These topics are well-covered at IGB.
- **Novel bigrams:** Two-word phrases (like "ecological stoichiometry" or "trait-based modeling") that appear in your query but not in existing IGB work.

### AI Literature Synthesis

Click the "Synthesize" button to get a citation-backed summary of the search results.

**Without an API key (extractive mode):**
- Pulls key sentences from abstracts and full-text chunks
- Organizes them with `[1]`, `[2]` citation markers
- Searches Semantic Scholar for supplementary papers from outside IGB
- Verifies all references exist on Semantic Scholar (green checkmark badges)

**With an Anthropic API key (generative mode):**
Uses a 3-step pipeline inspired by [OpenScholar](https://doi.org/10.1038/s41586-025-10072-4) (Asai et al., *Nature* 2026):

1. **Draft:** Claude generates a 2-4 paragraph synthesis with inline citations
2. **Self-critique:** The draft is reviewed for citation accuracy, coverage gaps, and balance
3. **Refinement:** The synthesis is rewritten addressing the critique

You can expand "View refinement process" to see the initial draft and the critique.

### Open Science Score

The Open Science tab ranks IGB researchers across 5 dimensions:

| Dimension | Weight | What it measures |
|-----------|--------|-----------------|
| OA Accessibility | 30% | Gold, hybrid, green, or closed access |
| Data Openness | 25% | Data availability statements, repository deposits |
| Code Openness | 20% | GitHub/GitLab links, code availability statements |
| Preprint Sharing | 10% | bioRxiv, EarthArXiv deposits |
| Repository Licensing | 15% | FRED data repository deposits with licenses |

**Key design:** Scores use **empirical Bayes shrinkage** — researchers with few publications are pulled toward the IGB average, so someone with 2 papers and 100% OA isn't ranked above someone with 50 papers and 80% OA. The formula:

```
adjusted_rate = (n/(n+k)) * observed + (k/(n+k)) * igb_mean
```

where `k=10`. With 2 papers, you're mostly showing the IGB average. With 50 papers, you're mostly showing the actual rate.

Confidence labels (High/Medium/Low) indicate how reliable each score is based on publication count.

### External Partners Map

The External tab shows a world map of IGB's 306 collaboration partners, with markers at each institution's actual location (not country centroids). Hover for details; institutions are sized by collaboration count.

### Scopefish

Scopefish is a companion tool mounted at `/scopefish/` that focuses on research intelligence beyond IGB's own publications — tracking what's happening in the broader freshwater science community.

### Citation Network

For the top 20 search results, Feuerstein fetches citation relationships from OpenAlex and draws an interactive network graph. Nodes are publications (sized by relevance), edges show "cites" relationships.

### Research Landscape

A t-SNE projection of all 4,730 publication embeddings at `/landscape`. Each dot is a paper. Papers close together are about similar topics. Color by department or year, filter by year range, search to highlight specific papers.

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Backend | Python 3.12, FastAPI | Async, fast, modern Python web framework |
| Frontend | HTML, CSS, Jinja2, Chart.js | Simple, no build step, server-rendered |
| Database | SQLite (flinstone.db, 83MB) | Zero config, single file, fast for <10K records |
| Embeddings | SPECTER2 via ONNX Runtime (768-dim) | Scientific paper embeddings, no GPU needed |
| Re-ranker | Cross-encoder via ONNX (87MB) | Higher accuracy for top candidates |
| Full text | pypdf for extraction, 400-word chunks | Pure Python, works on Windows ARM64 |
| Data source | OpenAlex API (free) | Open bibliographic data, no API key needed |
| Citation verification | Semantic Scholar API | Checks papers exist, finds supplementary literature |
| Hosting | Hugging Face Spaces (Docker) | Free, always-on, auto-deploys from git push |
| Version control | GitHub + Git LFS | LFS for large files (models, embeddings, DB) |

**No GPU, no paid APIs required for core functionality.** Everything runs on a CPU.

---

## File Structure

```
flinstone/
  app/
    main.py                # FastAPI routes and endpoint logic
    search.py              # Multi-stage search pipeline (embed + RRF + rerank)
    models.py              # SQLite data access layer
    analysis.py            # Collaborator ranking + TF-IDF novelty detection
    citations.py           # OpenAlex citation network builder
    synthesize.py          # AI literature synthesis (Claude API + S2)
    query_expansion.py     # Domain-specific abbreviation expansion
    templates/             # Jinja2 HTML templates
    static/style.css       # IGB-branded design system
  data/
    flinstone.db           # SQLite database (6,743 publications, 18,705 authors)
    embeddings_mpnet.npy   # 4,730 x 768 abstract embedding matrix
    chunk_embeddings.npy   # 10,063 x 768 full-text chunk embeddings
    pub_ids.npy            # Publication ID mapping for embeddings
    chunk_ids.npy          # Chunk ID mapping for chunk embeddings
    open_science.json      # Open science scores for all researchers
    institution_coords.json # 306 institution locations for external map
    network.json           # Pre-computed collaboration network
    model_specter2/        # SPECTER2 ONNX model (768-dim, 416MB)
    model_reranker/        # Cross-encoder ONNX model (87MB)
    model/                 # MiniLM fallback model (384-dim, 87MB)
  scopefish/               # Scopefish sub-app (bundled for Docker)
  scripts/
    fetch_openalex.py      # Pull IGB publications from OpenAlex
    build_embeddings.py    # Generate embedding vectors
    fetch_igb_staff.py     # Scrape IGB website for staff data
    compute_open_science.py # Calculate open science scores
    fetch_fulltext.py      # Download and chunk OA papers
    fetch_fulltext_fast.py # Faster full-text pipeline for known publishers
    fetch_external.py      # Fetch external collaboration data
    fetch_locations.py     # Get institution coordinates from OpenAlex
  Dockerfile               # Docker build for HF Spaces
  requirements.txt         # Python dependencies
```
