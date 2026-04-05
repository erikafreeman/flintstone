"""Scopefish — IGB Research Paper Discovery Tool."""

import csv
import io
import json
import os
import re
import uuid
from datetime import datetime
from typing import Optional

from markupsafe import Markup
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import models, search as sf_search

app = FastAPI(title="Scopefish")

BASE_DIR = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Feuerstein / FRED URLs (configurable)
FEUERSTEIN_URL = os.environ.get("FEUERSTEIN_URL", "http://localhost:7860")
FRED_URL = os.environ.get("FRED_URL", "https://fred.igb-berlin.de")

# URL prefix for mounting as sub-app (e.g. "/scopefish" when mounted under Feuerstein)
PREFIX = os.environ.get("SCOPEFISH_PREFIX", "")


def _md_bold(text):
    if not text:
        return ""
    return Markup(re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text))


def _score_color(score):
    """Return a CSS color for a relevance score 0-1."""
    if not score:
        return "#64748b"
    if score >= 0.7:
        return "#10b981"
    if score >= 0.4:
        return "#3b82f6"
    return "#8b5cf6"


def _score_pct(score):
    if not score:
        return 0
    return int(score * 100)


templates.env.filters["md_bold"] = _md_bold
templates.env.filters["score_color"] = _score_color
templates.env.filters["score_pct"] = _score_pct
templates.env.globals["flintstone_url"] = FEUERSTEIN_URL
templates.env.globals["fred_url"] = FRED_URL
templates.env.globals["now"] = datetime.now
templates.env.globals["prefix"] = PREFIX


DEPARTMENTS = {
    "dept1": {
        "id": "dept1",
        "name": "Ecohydrology & Biogeochemistry",
        "short": "Dept. 1",
        "db_name": "(Dept. 1) Ecohydrology and Biogeochemistry",
        "icon": "droplet",
        "color": "#06b6d4",
        "description": "Water-soil-atmosphere interactions, nutrient cycling, dissolved organic matter dynamics, and hydrological processes in freshwater systems.",
    },
    "dept2": {
        "id": "dept2",
        "name": "Community & Ecosystem Ecology",
        "short": "Dept. 2",
        "db_name": "(Dept. 2) Community and Ecosystem Ecology",
        "icon": "trees",
        "color": "#10b981",
        "description": "Biodiversity patterns, food webs, ecosystem functioning, and the effects of environmental change on freshwater communities.",
    },
    "dept3": {
        "id": "dept3",
        "name": "Plankton & Microbial Ecology",
        "short": "Dept. 3",
        "db_name": "(Dept. 3) Plankton and Microbial Ecology",
        "icon": "microscope",
        "color": "#8b5cf6",
        "description": "Microbial diversity, plankton dynamics, cyanobacteria blooms, and microbial processes in aquatic ecosystems.",
    },
    "dept4": {
        "id": "dept4",
        "name": "Fish Biology, Fisheries & Aquaculture",
        "short": "Dept. 4",
        "db_name": "(Dept. 4) Fish Biology, Fisheries and Aquaculture",
        "icon": "fish",
        "color": "#f59e0b",
        "description": "Fish ecology, population dynamics, fisheries management, aquaculture innovation, and conservation biology.",
    },
    "dept5": {
        "id": "dept5",
        "name": "Evolutionary & Integrative Ecology",
        "short": "Dept. 5",
        "db_name": "(Dept. 5) Evolutionary and Integrative Ecology",
        "icon": "dna",
        "color": "#ef4444",
        "description": "Evolutionary ecology, eco-genomics, adaptation, species interactions, and integrative approaches to understanding biodiversity.",
    },
}


@app.on_event("startup")
async def startup():
    conn = models.get_connection()
    models.init_db(conn)
    conn.close()
    # Try to load semantic search model
    try:
        sf_search.load()
    except Exception:
        pass  # Keyword search will be used as fallback
    # Start the background scheduler (pipeline + project fetching)
    from . import scheduler as sf_scheduler
    sf_scheduler.start()


@app.on_event("shutdown")
async def shutdown():
    from . import scheduler as sf_scheduler
    sf_scheduler.stop()


@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    conn = models.get_connection()
    try:
        stats = models.get_stats(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="landing.html",
        context={"stats": stats, "active": "home", "departments": DEPARTMENTS},
    )


@app.get("/digest", response_class=HTMLResponse)
async def digest_current(request: Request):
    conn = models.get_connection()
    try:
        digest = models.get_current_digest(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="digest.html",
        context={"digest": digest, "active": "digest", "departments": DEPARTMENTS},
    )


@app.get("/digest/{week}", response_class=HTMLResponse)
async def digest_week(request: Request, week: str):
    conn = models.get_connection()
    try:
        digest = models.get_digest(conn, week)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="digest.html",
        context={"digest": digest, "active": "digest", "departments": DEPARTMENTS},
    )


@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context={"active": "search", "departments": DEPARTMENTS},
    )


@app.post("/search", response_class=HTMLResponse)
async def search_results(
    request: Request,
    query: str = Form(...),
    top_k: int = Form(30),
    year_min: Optional[int] = Form(None),
    year_max: Optional[int] = Form(None),
    open_access: Optional[str] = Form(None),
    journal_tier: Optional[str] = Form(None),
    department: Optional[str] = Form(None),
):
    has_filters = any([year_min, year_max, open_access, journal_tier, department])
    dept_db_name = DEPARTMENTS[department]["db_name"] if department and department in DEPARTMENTS else None

    conn = models.get_connection()
    try:
        # Try semantic search first, fall back to keyword search
        semantic_results = sf_search.semantic_search(query, top_k=top_k * (3 if has_filters else 1))
        if semantic_results:
            sem_ids = [r[0] for r in semantic_results]
            sem_scores = {r[0]: r[1] for r in semantic_results}
            if has_filters:
                papers = models.search_papers_filtered(
                    conn, query, limit=top_k * 3,
                    year_min=year_min, year_max=year_max,
                    open_access=bool(open_access), journal_tier=journal_tier or None,
                    department=dept_db_name, paper_ids=sem_ids,
                )
                for p in papers:
                    p["semantic_score"] = sem_scores.get(p["id"], 0)
                papers.sort(key=lambda p: p.get("semantic_score", 0), reverse=True)
                papers = papers[:top_k]
            else:
                papers = models.get_papers_by_ids(conn, sem_ids)
                for p in papers:
                    p["semantic_score"] = sem_scores.get(p["id"], 0)
            search_mode = "semantic"
        else:
            if has_filters:
                papers = models.search_papers_filtered(
                    conn, query, limit=top_k,
                    year_min=year_min, year_max=year_max,
                    open_access=bool(open_access), journal_tier=journal_tier or None,
                    department=dept_db_name,
                )
            else:
                papers = models.search_papers(conn, query, limit=top_k)
            search_mode = "keyword"
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context={
            "query": query,
            "papers": papers,
            "top_k": top_k,
            "search_mode": search_mode,
            "active": "search",
            "departments": DEPARTMENTS,
            "year_min": year_min,
            "year_max": year_max,
            "open_access": open_access,
            "journal_tier": journal_tier or "",
            "department": department or "",
        },
    )


@app.get("/departments", response_class=HTMLResponse)
async def departments_page(request: Request):
    conn = models.get_connection()
    try:
        depts = models.get_departments(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="departments.html",
        context={"depts": depts, "active": "departments", "departments": DEPARTMENTS, "dept_meta": DEPARTMENTS},
    )


@app.get("/department/{dept_id}", response_class=HTMLResponse)
async def department_feed(request: Request, dept_id: str):
    meta = DEPARTMENTS.get(dept_id)
    if not meta:
        return HTMLResponse("Department not found", status_code=404)

    conn = models.get_connection()
    try:
        papers = models.get_department_feed(conn, meta["db_name"])
        analytics = models.get_department_analytics(conn, meta["db_name"])
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="department.html",
        context={
            "dept": meta,
            "papers": papers,
            "analytics": analytics,
            "active": "departments",
            "departments": DEPARTMENTS,
        },
    )


@app.get("/paper/{paper_id:path}", response_class=HTMLResponse)
async def paper_detail(request: Request, paper_id: str):
    conn = models.get_connection()
    try:
        paper = models.get_paper_detail(conn, paper_id)
        if paper:
            paper["enrichment"] = models.get_paper_enrichment(conn, paper_id)
            paper["social"] = models.get_paper_social(conn, paper_id)
            paper["altmetric"] = models.get_paper_altmetric(conn, paper_id)
    finally:
        conn.close()
    if not paper:
        return HTMLResponse("Paper not found", status_code=404)
    return templates.TemplateResponse(
        request=request,
        name="paper.html",
        context={"paper": paper, "active": "digest", "departments": DEPARTMENTS},
    )


@app.get("/researchers", response_class=HTMLResponse)
async def researchers_page(request: Request):
    conn = models.get_connection()
    try:
        researchers = models.get_tracked_researchers(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="researchers.html",
        context={"researchers": researchers, "active": "researchers", "departments": DEPARTMENTS},
    )


# === Headlines ===

@app.get("/headlines", response_class=HTMLResponse)
async def headlines_page(request: Request):
    conn = models.get_connection()
    try:
        headlines = models.get_headlines(conn, limit=15)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="headlines.html",
        context={"headlines": headlines, "active": "headlines", "departments": DEPARTMENTS},
    )


# === Projects ===

@app.get("/projects", response_class=HTMLResponse)
async def projects_page(request: Request, source: str = Query(None)):
    conn = models.get_connection()
    try:
        projects = models.get_funded_projects(conn, source=source, limit=100)
        stats = models.get_project_stats(conn)
        press = models.get_press_releases(conn, limit=20)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="projects.html",
        context={
            "projects": projects,
            "stats": stats,
            "press": press,
            "active_source": source,
            "active": "projects",
            "departments": DEPARTMENTS,
        },
    )


# === Funding ===

@app.get("/funding", response_class=HTMLResponse)
async def funding_page(request: Request):
    conn = models.get_connection()
    try:
        calls = models.get_funding_calls(conn)
    finally:
        conn.close()

    # Always include curated portals
    from .fetchers.funding import get_funding_portals
    portals = get_funding_portals()

    return templates.TemplateResponse(
        request=request,
        name="funding.html",
        context={
            "calls": calls,
            "portals": portals,
            "active": "funding",
            "departments": DEPARTMENTS,
        },
    )


# === Custom Feeds ===

@app.get("/feeds", response_class=HTMLResponse)
async def feeds_list(request: Request):
    conn = models.get_connection()
    try:
        feeds = models.get_custom_feeds(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="feeds.html",
        context={"feeds": feeds, "active": "feeds", "departments": DEPARTMENTS},
    )


@app.get("/feeds/new", response_class=HTMLResponse)
async def feed_new(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="feed_edit.html",
        context={"feed": None, "active": "feeds", "departments": DEPARTMENTS},
    )


@app.post("/feeds/new", response_class=HTMLResponse)
async def feed_create(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    keywords: str = Form(...),
):
    feed_id = str(uuid.uuid4())[:8]
    conn = models.get_connection()
    try:
        models.save_custom_feed(conn, feed_id, name.strip(), description.strip(), keywords.strip())
    finally:
        conn.close()
    return RedirectResponse(url=f"{PREFIX}/feeds/{feed_id}", status_code=303)


@app.get("/feeds/{feed_id}", response_class=HTMLResponse)
async def feed_view(request: Request, feed_id: str):
    conn = models.get_connection()
    try:
        feed = models.get_custom_feed(conn, feed_id)
        if not feed:
            return HTMLResponse("Feed not found", status_code=404)
        papers = models.get_custom_feed_papers(conn, feed["keyword_list"], limit=50)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="feed_view.html",
        context={
            "feed": feed,
            "papers": papers,
            "active": "feeds",
            "departments": DEPARTMENTS,
        },
    )


@app.get("/feeds/{feed_id}/edit", response_class=HTMLResponse)
async def feed_edit(request: Request, feed_id: str):
    conn = models.get_connection()
    try:
        feed = models.get_custom_feed(conn, feed_id)
    finally:
        conn.close()
    if not feed:
        return HTMLResponse("Feed not found", status_code=404)
    return templates.TemplateResponse(
        request=request,
        name="feed_edit.html",
        context={"feed": feed, "active": "feeds", "departments": DEPARTMENTS},
    )


@app.post("/feeds/{feed_id}/edit", response_class=HTMLResponse)
async def feed_update(
    request: Request,
    feed_id: str,
    name: str = Form(...),
    description: str = Form(""),
    keywords: str = Form(...),
):
    conn = models.get_connection()
    try:
        models.save_custom_feed(conn, feed_id, name.strip(), description.strip(), keywords.strip())
    finally:
        conn.close()
    return RedirectResponse(url=f"{PREFIX}/feeds/{feed_id}", status_code=303)


@app.post("/feeds/{feed_id}/delete")
async def feed_delete(request: Request, feed_id: str):
    conn = models.get_connection()
    try:
        models.delete_custom_feed(conn, feed_id)
    finally:
        conn.close()
    return RedirectResponse(url=f"{PREFIX}/feeds", status_code=303)


# === CSV Export ===

@app.post("/export/search")
async def export_search_csv(
    query: str = Form(...),
    top_k: int = Form(30),
    year_min: Optional[int] = Form(None),
    year_max: Optional[int] = Form(None),
    open_access: Optional[str] = Form(None),
    journal_tier: Optional[str] = Form(None),
    department: Optional[str] = Form(None),
):
    """Export search results as CSV."""
    has_filters = any([year_min, year_max, open_access, journal_tier, department])
    dept_db_name = DEPARTMENTS[department]["db_name"] if department and department in DEPARTMENTS else None

    conn = models.get_connection()
    try:
        if has_filters:
            papers = models.search_papers_filtered(
                conn, query, limit=top_k,
                year_min=year_min, year_max=year_max,
                open_access=bool(open_access), journal_tier=journal_tier or None,
                department=dept_db_name,
            )
        else:
            papers = models.search_papers(conn, query, limit=top_k)
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Rank", "Title", "Authors", "Year", "Journal", "DOI", "Relevance", "Concepts"])
    for i, paper in enumerate(papers, 1):
        writer.writerow([
            i,
            paper.get("title", ""),
            paper.get("authors_str", ""),
            paper.get("year", ""),
            paper.get("journal", ""),
            paper.get("doi", ""),
            f"{(paper.get('relevance_score') or 0):.0%}",
            "; ".join(paper.get("concepts", [])),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=scopefish_results.csv"},
    )


@app.get("/export/digest/{week}")
async def export_digest_csv(week: str):
    """Export a weekly digest as CSV."""
    conn = models.get_connection()
    try:
        digest = models.get_digest(conn, week)
    finally:
        conn.close()

    if not digest:
        return HTMLResponse("Digest not found", status_code=404)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Tier", "Title", "Authors", "Year", "Journal", "DOI", "Relevance", "Concepts"])
    for paper in digest.get("papers", []):
        concepts = [c["concept_name"] for c in paper.get("concepts", [])] if paper.get("concepts") else []
        writer.writerow([
            paper.get("tier", ""),
            paper.get("title", ""),
            paper.get("authors_str", ""),
            paper.get("year", ""),
            paper.get("journal", ""),
            paper.get("doi", ""),
            f"{(paper.get('relevance_score') or 0):.0%}",
            "; ".join(concepts),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=scopefish_digest_{week}.csv"},
    )


# === JSON API ===

@app.get("/api/papers")
async def api_papers(
    query: str = Query(None),
    department: str = Query(None),
    limit: int = Query(30),
):
    """JSON API: search or list papers."""
    conn = models.get_connection()
    try:
        if query:
            papers = models.search_papers(conn, query, limit=limit)
        elif department:
            dept_meta = DEPARTMENTS.get(department)
            if dept_meta:
                papers = models.get_department_feed(conn, dept_meta["db_name"], limit=limit)
            else:
                return JSONResponse({"error": "Unknown department"}, status_code=404)
        else:
            papers = models.search_papers(conn, "freshwater ecology", limit=limit)
    finally:
        conn.close()

    # Serialize (strip internal fields)
    return JSONResponse([
        {
            "id": p.get("id"),
            "title": p.get("title"),
            "abstract_short": p.get("abstract_short", ""),
            "year": p.get("year"),
            "journal": p.get("journal"),
            "doi": p.get("doi"),
            "relevance_score": p.get("relevance_score"),
            "concepts": p.get("concepts", []),
            "authors": p.get("authors_str", ""),
        }
        for p in papers
    ])


@app.get("/api/digest")
async def api_digest(week: str = Query(None)):
    """JSON API: get digest data."""
    conn = models.get_connection()
    try:
        if week:
            digest = models.get_digest(conn, week)
        else:
            digest = models.get_current_digest(conn)
    finally:
        conn.close()

    if not digest:
        return JSONResponse({"error": "No digest found"}, status_code=404)

    return JSONResponse({
        "week": digest.get("week"),
        "paper_count": digest.get("paper_count"),
        "high_count": digest.get("high_count"),
        "medium_count": digest.get("medium_count"),
        "notable_count": digest.get("notable_count"),
        "papers": [
            {
                "id": p.get("id"),
                "title": p.get("title"),
                "tier": p.get("tier"),
                "year": p.get("year"),
                "journal": p.get("journal"),
                "doi": p.get("doi"),
                "relevance_score": p.get("relevance_score"),
                "authors": p.get("authors_str", ""),
            }
            for p in digest.get("papers", [])
        ],
    })


@app.get("/api/headlines")
async def api_headlines(limit: int = Query(10)):
    """JSON API: get headline papers."""
    conn = models.get_connection()
    try:
        headlines = models.get_headlines(conn, limit=limit)
    finally:
        conn.close()

    return JSONResponse([
        {
            "id": p.get("id"),
            "title": p.get("title"),
            "year": p.get("year"),
            "journal": p.get("journal"),
            "doi": p.get("doi"),
            "headline_score": p.get("headline_score"),
            "altmetric_score": p.get("altmetric_score"),
            "authors": p.get("authors_str", ""),
        }
        for p in headlines
    ])


# === Scheduler Status & Manual Trigger ===

@app.get("/api/status")
async def api_status():
    """Return scheduler status (next runs, last run info)."""
    from . import scheduler as sf_scheduler
    return JSONResponse(sf_scheduler.get_status())


@app.post("/api/pipeline/run")
async def api_pipeline_trigger(request: Request):
    """Manually trigger an immediate pipeline run (requires SCOPEFISH_ADMIN_KEY)."""
    admin_key = os.environ.get("SCOPEFISH_ADMIN_KEY")
    if not admin_key:
        return JSONResponse(
            {"error": "Admin key not configured on server"},
            status_code=503,
        )

    # Accept key from Authorization header or JSON body
    auth_header = request.headers.get("authorization", "")
    provided_key = auth_header.removeprefix("Bearer ").strip()
    if not provided_key:
        try:
            body = await request.json()
            provided_key = body.get("key", "")
        except Exception:
            provided_key = ""

    if provided_key != admin_key:
        return JSONResponse({"error": "Invalid admin key"}, status_code=403)

    from . import scheduler as sf_scheduler
    try:
        sf_scheduler.trigger_pipeline_now()
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)

    return JSONResponse({"status": "Pipeline run triggered"})


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
            "departments": DEPARTMENTS,
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
            "departments": DEPARTMENTS,
        },
        status_code=500,
    )
