# Feuerstein: What's Next

*A roadmap for future development, roughly in order of value and feasibility.*

---

## Completed (March–April 2026)

### ~~1. Department-Level Metadata~~ ✅ DONE
Scraped IGB website staff directory (359 staff members). Matched 289 to OpenAlex authors. Department badges and "✓ current staff" indicators show on collaborator listings and author profiles. Department filter dropdown added to search.

### ~~2. Full-Text Search~~ ✅ DONE
SQLite FTS5 index with hyphen-aware tokenization. Keyword matches shown as "Additional Keyword Matches" below semantic results.

### ~~3. Citation Network Analysis~~ ✅ DONE
Fetches citation links from OpenAlex for top search results. Interactive canvas visualization shows which papers in the results cite each other.

### ~~4. Temporal Trends~~ ✅ DONE
Bar chart (Chart.js) shows year-by-year distribution of matching publications.

### ~~Interactive Research Landscape (from #12)~~ ✅ DONE
t-SNE projection of all 4,730 publication embeddings. Interactive canvas at /landscape with:
- Color by department or year
- Year range slider filter
- Search to highlight specific publications
- Hover for details, click to open DOI

---

## Near-Term (April–May 2026)

### 1. LDB Integration for Department Data
**Status:** Waiting on meeting with Jana (IGB Library) in April

The web-scraped department data covers current staff (289 matched). The internal Leistungsdatenbank (LDB) would give us historical department assignments for all authors, not just current staff.

---

## Medium-Term (Summer 2026)

### 5. Better Embedding Model
**Upgrade from all-MiniLM-L6-v2 to a domain-specific model**

Options:
- `SPECTER2` — trained specifically on scientific papers (better at matching research abstracts)
- `all-mpnet-base-v2` — larger general model, ~20% more accurate
- Still runs locally via ONNX, just a model file swap

### 6. Abstract Generation / Gap Summary
**Use a large language model to write a structured novelty report**

Instead of just listing novel terms, we could feed the query + top results to Claude/GPT and get a narrative like:
> "Your proposed study of DOM molecular composition using FT-ICR-MS connects to a strong tradition of DOM research at IGB (particularly by Goldhammer, Zark, and Singer). However, the application of ecological theory — specifically niche theory and community assembly frameworks — to molecular-level patterns is not represented in IGB's current publication record. This 'Ecology of Molecules' framing appears genuinely novel."

### 7. Persistent Deployment
**Move from laptop to a server**

Options:
- Docker container on an IGB internal server
- Render.com or Railway.app (free tier, always-on)
- IGB's own infrastructure (talk to IT)

### 8. Slack Bot / API Endpoint
**Quick queries without opening a browser**

A `/feuerstein dom carbon cycling lakes` Slack command that returns the top 5 results and collaborators inline.

---

## Long-Term (Autumn 2026+)

### 9. ERC/DFG Proposal Writing Integration
**"Prior art" section for grant proposals**

When writing a proposal, automatically generate a "State of the Art at IGB" section with properly formatted citations from the top results.

### 10. Multi-Institution Search
**Expand beyond IGB**

OpenAlex has data for every institution. Could extend to:
- Partner institutions (FU Berlin, HU Berlin, TU Berlin)
- All Leibniz Institutes
- Custom institution lists for specific collaborations

### 11. Research Recommendation Engine
**Proactive suggestions**

Instead of the researcher coming to Feuerstein, Feuerstein comes to them:
- Weekly email: "New IGB papers related to your research profile"
- Alert when a new paper is published that's highly similar to your recent work
- "You might want to talk to X — they just published something related"

### 12. Interactive Visualization
**Map the IGB research landscape**

A 2D projection (UMAP/t-SNE) of all publication embeddings, colored by department or topic, with the user's query shown as a point. Shows at a glance where the research idea sits relative to IGB's full portfolio.

---

## Ideas Contributed by Others
*(Add ideas from colleagues here as they come in)*

- ...
