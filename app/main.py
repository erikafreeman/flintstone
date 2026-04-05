"""Feuerstein — IGB Publication Intelligence Tool."""

import csv
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Optional
from collections import Counter
from markupsafe import Markup
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import search, models, analysis, citations, synthesize
from . import similar as similar_mod
from . import citation_tracker, rag, reading_list, alerts, gaps

# Import Scopefish sub-app for mounting
os.environ["SCOPEFISH_PREFIX"] = "/scopefish"
scopefish_app = None
# Look for scopefish: first inside repo (Docker), then sibling dir (local dev)
_sf_candidates = [
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "scopefish")),
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "scopefish")),
]
for _sf_base in _sf_candidates:
    if os.path.isdir(os.path.join(_sf_base, "sfapp")):
        try:
            import sys
            sys.path.insert(0, _sf_base)
            import sfapp.main as _sf_main
            scopefish_app = _sf_main.app
            _sf_main.templates.env.globals["prefix"] = "/scopefish"
            _sf_main.templates.env.globals["feuerstein_url"] = "/"
            logging.info(f"Loaded Scopefish from {_sf_base}")
            break
        except Exception as e:
            logging.warning(f"Could not load Scopefish from {_sf_base}: {e}")
            scopefish_app = None

# Query logging
QUERY_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "query_log.jsonl")

def _log_query(query: str, top_k: int, n_results: int, year_min=None, year_max=None):
    """Append query to the log file."""
    try:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "query": query[:500],
            "top_k": top_k,
            "n_results": n_results,
            "year_min": year_min,
            "year_max": year_max,
        }
        with open(QUERY_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # Don't let logging break the app

app = FastAPI(title="Feuerstein")

BASE_DIR = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
if scopefish_app:
    app.mount("/scopefish", scopefish_app)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


def _md_bold(text):
    """Convert **bold** markdown to <strong> tags."""
    return Markup(re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text))

def _strip_html(text: str) -> str:
    if not text:
        return text
    return re.sub(r'<[^>]+>', '', text)

templates.env.filters["md_bold"] = _md_bold


@app.on_event("startup")
async def startup():
    search.load()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    conn = models.get_connection()
    try:
        stats = models.get_stats(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(request=request, name="index.html", context={"stats": stats, "active": "search"})


def _run_search(query, top_k, year_min, year_max):
    """Shared search logic used by both the HTML and CSV endpoints."""
    results = search.search(query, top_k=top_k, year_min=year_min, year_max=year_max)
    pub_ids = [r[0] for r in results]
    scores = {r[0]: r[1] for r in results}

    conn = models.get_connection()
    try:
        publications = models.get_publications_by_ids(conn, pub_ids)
        igb_authors = models.get_igb_authors_for_publications(conn, pub_ids)
        all_authors = models.get_all_authors_for_publications(conn, pub_ids)

        for pub in publications:
            pub["score"] = scores.get(pub["id"], 0)
            pub["score_pct"] = int(pub["score"] * 100)
            pub["authors_list"] = all_authors.get(pub["id"], [])
            abstract = _strip_html(pub.get("abstract") or "")
            pub["abstract_short"] = abstract[:200] + ("..." if len(abstract) > 200 else "")

        collaborators = analysis.rank_collaborators(pub_ids, igb_authors, publications)
        novelty = analysis.detect_novelty(query, publications)

        # Full-text search results (keyword complement)
        fts_results = models.fulltext_search(conn, query, limit=10)
        semantic_ids = set(pub_ids)
        fts_extra = [r for r in fts_results if r["id"] not in semantic_ids]

        # Year trend data
        year_counts = Counter()
        for pub in publications:
            if pub.get("year"):
                year_counts[pub["year"]] += 1

        # Citation network (for top 20 results)
        citation_data = citations.fetch_citation_links(pub_ids[:20])

    finally:
        conn.close()

    return publications, collaborators, novelty, fts_extra, year_counts, citation_data


@app.post("/search", response_class=HTMLResponse)
async def do_search(
    request: Request,
    query: str = Form(...),
    top_k: int = Form(20),
    year_min: Optional[int] = Form(None),
    year_max: Optional[int] = Form(None),
):
    publications, collaborators, novelty, fts_extra, year_counts, citation_data = _run_search(
        query, top_k, year_min, year_max
    )

    # Log the query
    _log_query(query, top_k, len(publications), year_min, year_max)

    # Build year trend for chart
    if year_counts:
        min_yr = min(year_counts.keys())
        max_yr = max(year_counts.keys())
        year_labels = list(range(min_yr, max_yr + 1))
        year_data = [year_counts.get(y, 0) for y in year_labels]
    else:
        year_labels, year_data = [], []

    # Build citation network data for the frontend
    citation_edges = citation_data.get("edges", [])
    citation_stats = citation_data.get("stats", {})

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "query": query,
            "publications": publications,
            "collaborators": collaborators,
            "novelty": novelty,
            "fts_extra": fts_extra,
            "top_k": top_k,
            "year_min": year_min,
            "year_max": year_max,
            "year_labels": year_labels,
            "year_data": year_data,
            "citation_edges": citation_edges,
            "citation_stats": citation_stats,
            "active": "search",
        },
    )


@app.post("/export")
async def export_csv(
    query: str = Form(...),
    top_k: int = Form(20),
    year_min: Optional[int] = Form(None),
    year_max: Optional[int] = Form(None),
):
    """Export search results as CSV."""
    publications, collaborators, novelty, fts_extra, year_counts, _ = _run_search(
        query, top_k, year_min, year_max
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Rank", "Similarity", "Title", "Authors", "Year", "Journal", "Citations", "DOI"])

    for i, pub in enumerate(publications, 1):
        authors_str = "; ".join(a["display_name"] for a in pub.get("authors_list", []))
        writer.writerow([
            i,
            f"{pub['score']:.2%}",
            pub.get("title", ""),
            authors_str,
            pub.get("year", ""),
            pub.get("journal", ""),
            pub.get("cited_by_count", 0),
            pub.get("doi", ""),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=feuerstein_results.csv"},
    )


@app.post("/api/synthesize")
async def api_synthesize(
    query: str = Form(...),
    top_k: int = Form(10),
    year_min: Optional[int] = Form(None),
    year_max: Optional[int] = Form(None),
):
    """Generate a citation-backed synthesis of search results.

    Works in two modes:
    - With ANTHROPIC_API_KEY: full LLM synthesis with self-feedback loop
    - Without API key: extractive synthesis from abstracts + S2 verification
    """
    publications, _, _, _, _, _ = _run_search(query, top_k, year_min, year_max)
    result = synthesize.synthesize(query, publications, refine=True)
    return result


@app.get("/api/synthesize/status")
async def synthesize_status():
    """Check synthesis mode (generative with API key, extractive without)."""
    return {
        "available": True,
        "mode": "generative" if os.environ.get("ANTHROPIC_API_KEY") else "extractive",
    }


@app.get("/author/{author_id:path}", response_class=HTMLResponse)
async def author_profile(request: Request, author_id: str):
    conn = models.get_connection()
    try:
        author = models.get_author(conn, author_id)
        if not author:
            return templates.TemplateResponse(
                request=request, name="author.html", context={"author": None},
            )

        publications = models.get_publications_by_author(conn, author_id)
        coauthors = models.get_coauthors(conn, author_id)

        for pub in publications:
            abstract = _strip_html(pub.get("abstract") or "")
            pub["abstract_short"] = abstract[:150] + ("..." if len(abstract) > 150 else "")

        years = [p["year"] for p in publications if p.get("year")]
        total_citations = sum(p.get("cited_by_count", 0) for p in publications)
    finally:
        conn.close()

    return templates.TemplateResponse(
        request=request,
        name="author.html",
        context={
            "author": author,
            "publications": publications,
            "coauthors": coauthors,
            "total_citations": total_citations,
            "year_range": f"{min(years)}–{max(years)}" if years else "N/A",
            "pub_count": len(publications),
        },
    )


@app.get("/map", response_class=HTMLResponse)
async def sample_map(request: Request):
    return templates.TemplateResponse(request=request, name="map.html", context={"active": "map"})


@app.get("/network", response_class=HTMLResponse)
async def network(request: Request):
    return templates.TemplateResponse(request=request, name="network.html", context={"active": "network"})


@app.get("/opensci", response_class=HTMLResponse)
async def opensci(request: Request):
    return templates.TemplateResponse(request=request, name="opensci.html", context={"active": "opensci"})


@app.get("/methodology", response_class=HTMLResponse)
async def methodology(request: Request):
    return templates.TemplateResponse(request=request, name="methodology.html", context={"active": "opensci"})


@app.get("/landscape", response_class=HTMLResponse)
async def landscape(request: Request):
    return templates.TemplateResponse(request=request, name="landscape.html", context={"active": "landscape"})


@app.get("/external", response_class=HTMLResponse)
async def external(request: Request):
    conn = models.get_connection()
    try:
        stats, top_institutions, collaborators, inst_coords = models.get_external_data(conn)
    finally:
        conn.close()

    return templates.TemplateResponse(
        request=request,
        name="external.html",
        context={
            "stats": stats,
            "top_institutions": top_institutions,
            "collaborators": collaborators,
            "institution_coords": inst_coords,
            "active": "external",
        },
    )


# === Similar Papers ===

@app.get("/similar/{pub_id:path}", response_class=HTMLResponse)
async def similar_papers(request: Request, pub_id: str):
    """Find papers semantically similar to a given publication."""
    conn = models.get_connection()
    try:
        pub = models.get_publication(conn, pub_id)
        if not pub:
            return templates.TemplateResponse(
                request=request, name="error.html",
                context={"error_code": 404, "error_title": "Not Found",
                         "error_message": "Publication not found.", "active": ""},
                status_code=404,
            )

        # Get similar papers
        results = similar_mod.find_similar(pub_id, top_k=20)
        sim_ids = [r[0] for r in results]
        sim_scores = {r[0]: r[1] for r in results}

        sim_pubs = models.get_publications_by_ids(conn, sim_ids)
        all_authors = models.get_all_authors_for_publications(conn, sim_ids)

        for p in sim_pubs:
            p["score"] = sim_scores.get(p["id"], 0)
            p["score_pct"] = int(p["score"] * 100)
            p["authors_list"] = all_authors.get(p["id"], [])
            abstract = _strip_html(p.get("abstract") or "")
            p["abstract_short"] = abstract[:200] + ("..." if len(abstract) > 200 else "")

        # Source publication authors
        source_authors = models.get_all_authors_for_publications(conn, [pub_id])
        pub["authors_list"] = source_authors.get(pub_id, [])
    finally:
        conn.close()

    return templates.TemplateResponse(
        request=request, name="similar.html",
        context={"pub": pub, "similar": sim_pubs, "active": "search"},
    )


# === Citation Tracker ===

@app.get("/citations/{pub_id:path}", response_class=HTMLResponse)
async def citation_tracker_page(request: Request, pub_id: str):
    """Show who cited an IGB paper."""
    conn = models.get_connection()
    try:
        pub = models.get_publication(conn, pub_id)
        if not pub:
            return templates.TemplateResponse(
                request=request, name="error.html",
                context={"error_code": 404, "error_title": "Not Found",
                         "error_message": "Publication not found.", "active": ""},
                status_code=404,
            )
        source_authors = models.get_all_authors_for_publications(conn, [pub_id])
        pub["authors_list"] = source_authors.get(pub_id, [])
    finally:
        conn.close()

    # Fetch citing papers from OpenAlex
    citing_data = citation_tracker.get_citing_papers(pub_id, limit=50)
    co_cited = citation_tracker.get_co_cited_papers(pub_id, limit=15)

    return templates.TemplateResponse(
        request=request, name="citations.html",
        context={
            "pub": pub,
            "citing_papers": citing_data["citing_papers"],
            "stats": citing_data["stats"],
            "co_cited": co_cited,
            "active": "search",
        },
    )


# === RAG Research Assistant ===

@app.get("/ask", response_class=HTMLResponse)
async def ask_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="ask.html",
        context={"active": "ask"},
    )


@app.post("/ask", response_class=HTMLResponse)
async def ask_question(request: Request, question: str = Form(...)):
    result = rag.ask(question)
    return templates.TemplateResponse(
        request=request, name="ask.html",
        context={
            "question": question,
            "answer": result.get("answer", ""),
            "sources": result.get("sources", []),
            "mode": result.get("mode", ""),
            "chunks_used": result.get("chunks_used", 0),
            "active": "ask",
        },
    )


@app.post("/api/ask")
async def api_ask(question: str = Form(...)):
    """JSON API for the research assistant."""
    result = rag.ask(question)
    return JSONResponse(result)


# === Reading Lists ===

@app.get("/reading-list", response_class=HTMLResponse)
async def reading_list_page(request: Request, list_id: int = Query(None)):
    lists = reading_list.get_lists()
    if not lists:
        reading_list.create_list("My Reading List", "Default reading list")
        lists = reading_list.get_lists()

    active_list = list_id or (lists[0]["id"] if lists else None)
    items = reading_list.get_list_items(active_list) if active_list else []

    return templates.TemplateResponse(
        request=request, name="reading_list.html",
        context={
            "lists": lists,
            "active_list": active_list,
            "items": items,
            "active": "reading-list",
        },
    )


@app.post("/reading-list/add")
async def reading_list_add(
    list_id: int = Form(...),
    publication_id: str = Form(...),
    note: str = Form(""),
):
    success = reading_list.add_item(list_id, publication_id, note)
    return JSONResponse({"success": success, "message": "Added to reading list" if success else "Already in list"})


@app.post("/reading-list/remove")
async def reading_list_remove(
    list_id: int = Form(...),
    publication_id: str = Form(...),
):
    success = reading_list.remove_item(list_id, publication_id)
    return JSONResponse({"success": success})


@app.post("/reading-list/note")
async def reading_list_update_note(
    list_id: int = Form(...),
    publication_id: str = Form(...),
    note: str = Form(""),
):
    success = reading_list.update_note(list_id, publication_id, note)
    return JSONResponse({"success": success})


@app.post("/reading-list/create")
async def reading_list_create(
    name: str = Form(...),
    description: str = Form(""),
):
    list_id = reading_list.create_list(name, description)
    return JSONResponse({"success": True, "list_id": list_id})


@app.get("/reading-list/export/csv")
async def reading_list_export_csv(list_id: int = Query(...)):
    csv_data = reading_list.export_csv(list_id)
    return StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=reading_list_{list_id}.csv"},
    )


@app.get("/reading-list/export/bibtex")
async def reading_list_export_bibtex(list_id: int = Query(...)):
    bib_data = reading_list.export_bibtex(list_id)
    return StreamingResponse(
        iter([bib_data]),
        media_type="application/x-bibtex",
        headers={"Content-Disposition": f"attachment; filename=reading_list_{list_id}.bib"},
    )


# === Author & Topic Alerts ===

@app.get("/alerts", response_class=HTMLResponse)
async def alerts_page(request: Request):
    all_alerts = alerts.get_alerts()
    # Get results for each alert
    for alert in all_alerts:
        alert["results"] = alerts.get_alert_results(alert["id"])[:10]
    return templates.TemplateResponse(
        request=request, name="alerts.html",
        context={"alerts": all_alerts, "active": "alerts"},
    )


@app.post("/alerts/add")
async def alerts_add(
    alert_type: str = Form(...),
    name: str = Form(...),
    value: str = Form(...),
):
    result = alerts.add_alert(alert_type, name, value)
    if "error" in result:
        return JSONResponse(result, status_code=400)
    return JSONResponse(result)


@app.post("/alerts/remove")
async def alerts_remove(alert_id: int = Form(...)):
    success = alerts.remove_alert(alert_id)
    return JSONResponse({"success": success})


@app.post("/alerts/check")
async def alerts_check(alert_id: int = Form(None)):
    if alert_id:
        count = alerts.check_alert(alert_id)
        return JSONResponse({"new_papers": count})
    else:
        results = alerts.check_all_alerts()
        return JSONResponse({"results": results})


@app.post("/alerts/mark-read")
async def alerts_mark_read(alert_id: int = Form(...)):
    alerts.mark_read(alert_id)
    return JSONResponse({"success": True})


@app.get("/api/authors/search")
async def api_search_authors(q: str = Query(...)):
    """Search OpenAlex for authors (for alert setup)."""
    results = alerts.search_authors(q)
    return JSONResponse(results)


# === Research Gap Finder ===

@app.get("/gaps", response_class=HTMLResponse)
async def gaps_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="gaps.html",
        context={"active": "gaps"},
    )


@app.post("/gaps", response_class=HTMLResponse)
async def gaps_analyze(request: Request):
    gap_data = gaps.find_gaps(max_domains=15)
    return templates.TemplateResponse(
        request=request, name="gaps.html",
        context={
            "gap_data": gap_data,
            "active": "gaps",
        },
    )


@app.get("/api/gaps")
async def api_gaps():
    """JSON API for gap analysis."""
    gap_data = gaps.find_gaps(max_domains=15)
    return JSONResponse(gap_data)


# === Error Handlers ===

@app.exception_handler(404)
async def not_found(request: Request, exc):
    return templates.TemplateResponse(
        request=request,
        name="error.html",
        context={
            "error_code": 404,
            "error_title": "Page Not Found",
            "error_message": "The page you're looking for doesn't exist or may have been moved.",
            "active": "",
        },
        status_code=404,
    )


@app.exception_handler(500)
async def server_error(request: Request, exc):
    return templates.TemplateResponse(
        request=request,
        name="error.html",
        context={
            "error_code": 500,
            "error_title": "Server Error",
            "error_message": "Something went wrong. Please try again later.",
            "active": "",
        },
        status_code=500,
    )
