"""Scopefish — IGB Research Paper Discovery Tool."""

import json
import os
import re
import uuid
from datetime import datetime
from typing import Optional

from markupsafe import Markup
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import models

app = FastAPI(title="Scopefish")

BASE_DIR = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Flintstone / FRED URLs (configurable)
FLINTSTONE_URL = os.environ.get("FLINTSTONE_URL", "http://localhost:7860")
FRED_URL = os.environ.get("FRED_URL", "https://fred.igb-berlin.de")

# URL prefix for mounting as sub-app (e.g. "/scopefish" when mounted under Flintstone)
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
templates.env.globals["flintstone_url"] = FLINTSTONE_URL
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
):
    conn = models.get_connection()
    try:
        papers = models.search_papers(conn, query, limit=top_k)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context={
            "query": query,
            "papers": papers,
            "top_k": top_k,
            "active": "search",
            "departments": DEPARTMENTS,
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
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="department.html",
        context={
            "dept": meta,
            "papers": papers,
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
