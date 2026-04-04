# Feuerstein: Ideas for Improvement

*Roughly organized by effort and impact. Updated April 2026.*

---

## Quick Wins (Days)

### 1. Better Open Science Scoring Data
**Problem:** The open science score currently infers OA status and data/code sharing from OpenAlex metadata, which is incomplete. Many papers with data in FRED or code on GitHub aren't detected.

**Fix:** Cross-reference with FRED API directly (not just DOI matching). Parse GitHub/GitLab links from paper PDFs. Use Unpaywall API for more accurate OA classification.

**Impact:** More accurate and fair scores. Researchers would trust the rankings more.

---

### 2. Author Disambiguation
**Problem:** Some authors have multiple OpenAlex IDs (e.g., "Dörthe Tetzlaff" vs "Doerthe Tetzlaff"). We handle umlauts now, but name changes (marriage, etc.) and common names still cause splits.

**Fix:** Use ORCID as primary key where available. OpenAlex provides ORCID links for many authors. Fall back to name matching only when ORCID is missing.

**Impact:** More accurate collaborator rankings and author profiles.

---

### 3. Search Result Explanations
**Problem:** Users see a similarity percentage but don't know *why* a paper matched. Was it the abstract? A specific chunk? A keyword?

**Fix:** For each result, show which search source contributed most (abstract semantic, chunk semantic, keyword). Highlight matching terms in the abstract.

**Impact:** Builds trust. Researchers can judge whether the match is meaningful.

---

### 4. Saved Searches / Alerts
**Problem:** Researchers search once, then forget about Feuerstein. No reason to come back.

**Fix:** Let users save a query and get notified (email or RSS) when new IGB publications match it. Run saved searches weekly against new OpenAlex data.

**Impact:** Turns Feuerstein from a one-time tool into a continuous research companion.

---

## Medium Effort (Weeks)

### 5. LDB Integration
**Status:** Waiting on meeting with Jana (IGB Library)

The internal Leistungsdatenbank (LDB) has historical department assignments for all authors, not just current staff. This would let us show department info for retired/departed researchers too.

**Impact:** Complete department coverage instead of current-staff-only.

---

### 6. Smarter Synthesis with RAG
**Current state:** The synthesis feature uses Claude API with top-10 results as context.

**Improvements inspired by OpenScholar:**
- **Retrieval during feedback:** When the self-critique identifies gaps, automatically search Semantic Scholar for additional evidence and incorporate it in the refined version.
- **Posthoc citation attribution:** After generating text, verify each `[N]` citation actually supports the claim it's attached to. Flag or fix unsupported claims.
- **OpenScholar's reranker model** (`OpenSciLM/OpenScholar_Reranker`): A 0.6B parameter reranker trained specifically for scientific literature. Could replace our generic cross-encoder for better scientific relevance scoring.

**Impact:** More reliable, better-cited synthesis. Catches hallucinated claims.

---

### 7. Department-Level Dashboards
**Idea:** Each IGB department gets its own page showing:
- Publication trends over time
- Top collaborating institutions
- Most-cited papers
- Open science score distribution
- Research topic clusters (using embedding visualization)

**Impact:** Useful for department heads and annual reporting.

---

### 8. Grant Proposal Support
**Idea:** A "State of the Art at IGB" generator for grant proposals.

Paste your proposal abstract, and Feuerstein generates a properly formatted literature review section citing IGB publications. Outputs in formats ready for DFG, ERC, or BMBF proposals.

**Impact:** Saves hours of literature search during proposal writing.

---

### 9. Multi-Language Query Support
**Problem:** Some older IGB publications have German titles/abstracts. SPECTER2 is English-only.

**Fix:** Add a multilingual embedding model (e.g., `paraphrase-multilingual-MiniLM-L12-v2`) as a secondary search path. Detect query language and route accordingly.

**Impact:** Better coverage of the full IGB publication history.

---

## Larger Projects (Months)

### 10. Multi-Institution Search
OpenAlex has data for every institution. Feuerstein could expand to:
- Partner institutions (FU Berlin, HU Berlin, TU Berlin)
- All Leibniz Institutes
- Custom institution lists for specific collaborations
- Compare IGB's research profile against peer institutes

**Technical:** Would need a larger database and possibly FAISS for approximate nearest-neighbor search (cosine similarity over 100K+ embeddings is still fast, but 1M+ would need indexing).

---

### 11. Research Recommendation Engine
Instead of researchers coming to Feuerstein, Feuerstein comes to them:

- **Weekly digest:** "New IGB papers related to your research profile"
- **Collaboration alerts:** "Dr. X just published something very similar to your recent work — you might want to connect"
- **Trend detection:** "Publications on microplastics at IGB increased 40% this year"
- **Delivery:** Email, Slack bot, or RSS feed

**Technical:** Build researcher profiles from their publication embeddings. Compare new papers against all profiles. Send notifications for high-similarity matches.

---

### 12. Slack / Teams Bot
A `/feuerstein DOM carbon cycling lakes` command that returns the top 5 results and suggested collaborators inline. No browser needed.

---

### 13. Interactive Knowledge Graph
Go beyond the current citation network to build a full knowledge graph:
- Papers linked by shared concepts (not just citations)
- Author clusters by research similarity
- Temporal evolution of research themes
- "How is researcher A connected to researcher B?" path finding

**Technical:** Extract entities (species, methods, locations, chemicals) from full-text chunks using NER. Build a graph database (Neo4j or similar).

---

### 14. Benchmark Against Other Tools
Compare Feuerstein's search quality against:
- Google Scholar (baseline)
- Semantic Scholar
- OpenScholar demo (open-scholar.allen.ai)
- Elicit, Consensus, scite

Create a test set of 20-30 queries from IGB researchers with known relevant papers. Measure precision@10 and recall@20. Use this to identify where Feuerstein underperforms and target improvements.

---

### 15. Fine-Tune the Embedding Model
SPECTER2 is trained on general scientific papers. Fine-tuning on freshwater ecology literature could improve retrieval quality for IGB-specific queries.

**Approach:**
- Use IGB's citation network as training signal (papers that cite each other should be closer in embedding space)
- Contrastive learning: positive pairs = papers by same author on same topic, negative pairs = random
- Would need ~10K training pairs

**Technical:** Requires GPU for training, but inference stays on CPU via ONNX export.

---

## Architecture Improvements

### A. Automated Data Refresh
Currently, updating the database requires manually running scripts. Set up a weekly cron job (or HF Spaces scheduled task) that:
1. Fetches new IGB publications from OpenAlex
2. Embeds new abstracts
3. Downloads and chunks new OA papers
4. Updates open science scores
5. Refreshes staff data from IGB website

---

### B. API Endpoints
Expose Feuerstein's search as a REST API so other tools can use it:
- `GET /api/search?q=...` — returns JSON results
- `GET /api/author/{id}` — author profile as JSON
- `GET /api/similar/{pub_id}` — find similar papers

This enables the Slack bot, email alerts, and integration with other IGB tools.

---

### C. User Accounts and Feedback
Let researchers log in (IGB SSO?) and:
- Save favorite searches
- Mark results as relevant/irrelevant (improves ranking over time)
- Add notes to papers
- Build personal reading lists

The relevance feedback could be used to fine-tune the embedding model (see #15).

---

### D. Performance Monitoring
Add lightweight analytics to understand usage:
- Which queries are most common?
- Which results get clicked?
- Where do users drop off?
- Are synthesis results useful?

Query logging already exists (`data/query_log.jsonl`). Add click tracking and synthesis feedback buttons.

---

## Ideas From the OpenScholar Paper

The OpenScholar paper (Asai et al., *Nature* 2026) demonstrated several techniques we partially use but could improve:

| OpenScholar Feature | Feuerstein Status | Gap |
|---|---|---|
| Multi-source retrieval | 3-way RRF (abstract + chunk + FTS) | Could add Semantic Scholar as 4th source |
| Cross-encoder reranking | Generic cross-encoder (87MB) | Could use OpenScholar's science-specific reranker |
| Self-feedback loop | Implemented via Claude API | Could add retrieval during feedback (search for missing evidence) |
| Citation verification | S2 title matching | Could verify claim-level support (does [3] actually support the sentence?) |
| 45M paper data store | 6,743 IGB papers | Could add S2 supplementary search (already partially done) |
| Posthoc attribution | Not implemented | LLM checks each sentence against sources |
| Training data | Using off-the-shelf models | Could fine-tune on IGB citation graph |

---

## Completed Features

For reference, these are already built and deployed:

- Semantic search with SPECTER2 embeddings (768-dim)
- Full-text chunk search (10,063 chunks from 296+ OA papers)
- 3-way reciprocal rank fusion (abstract + chunk + keyword)
- Cross-encoder re-ranking
- Query expansion (59 freshwater ecology abbreviations)
- IGB staff matching with department badges
- Collaborator ranking
- TF-IDF novelty assessment
- Citation network visualization
- Open Science Score (5 dimensions, empirical Bayes shrinkage)
- External partners map (306 institutions with coordinates)
- Research landscape t-SNE visualization
- AI literature synthesis (extractive + generative with self-feedback)
- Semantic Scholar citation verification and supplementary retrieval
- Scopefish sub-app integration
- Deployed on Hugging Face Spaces (Docker)
