"""Fetch funded research projects from CORDIS, NSF Awards, and OpenAlex."""

import logging
import re
import time
import requests

log = logging.getLogger(__name__)

# ── CORDIS (EU Funded Projects) ─────────────────────────────────────────

CORDIS_URL = "https://cordis.europa.eu/search/en"

CORDIS_QUERIES = [
    "freshwater ecology",
    "aquatic biodiversity",
    "lake ecosystem",
    "river restoration",
    "water quality monitoring",
    "environmental DNA aquatic",
    "fish population dynamics",
    "plankton ecology",
]


def fetch_cordis_projects(max_per_query: int = 10, since_year: int = 2020) -> list:
    """Fetch freshwater-related EU funded projects from CORDIS."""
    projects = []
    seen_ids = set()

    for query in CORDIS_QUERIES:
        try:
            full_query = f"{query} AND startDate>='{since_year}-01-01'"
            resp = requests.get(
                CORDIS_URL,
                params={
                    "q": full_query,
                    "type": "project",
                    "num": max_per_query,
                    "format": "json",
                    "srt": "Relevance:decreasing",
                },
                headers={"Accept": "application/json"},
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            data = resp.json()
            hits = data.get("hits", {}).get("hit", [])
            if not isinstance(hits, list):
                hits = [hits]

            for hit in hits:
                proj = hit.get("project", {})
                proj_id = proj.get("id", "")
                if not proj_id or proj_id in seen_ids:
                    continue
                seen_ids.add(proj_id)

                # Extract coordinator from relations
                coordinator = ""
                relations = proj.get("relations", {})
                if isinstance(relations, dict):
                    associations = relations.get("associations", {})
                    if isinstance(associations, dict):
                        org = associations.get("organization", {})
                        if isinstance(org, list):
                            for o in org:
                                if o.get("type") == "coordinator":
                                    coordinator = o.get("legalName", "")
                                    break
                        elif isinstance(org, dict):
                            coordinator = org.get("legalName", "")

                projects.append({
                    "source": "cordis",
                    "project_id": f"cordis-{proj_id}",
                    "title": proj.get("title", ""),
                    "acronym": proj.get("acronym", ""),
                    "description": (proj.get("teaser") or proj.get("objective", ""))[:500],
                    "funder": "European Commission",
                    "programme": _extract_programme(proj),
                    "total_cost": proj.get("totalCost", ""),
                    "start_date": proj.get("startDate", ""),
                    "end_date": proj.get("endDate", ""),
                    "status": proj.get("status", ""),
                    "coordinator": coordinator,
                    "url": f"https://cordis.europa.eu/project/id/{proj_id}",
                    "keywords": proj.get("keywords", ""),
                })

            time.sleep(0.3)
        except Exception as e:
            log.warning(f"CORDIS fetch error for '{query}': {e}")
            continue

    return projects


def _extract_programme(proj: dict) -> str:
    relations = proj.get("relations", {})
    if isinstance(relations, dict):
        cats = relations.get("categories", {})
        if isinstance(cats, dict):
            cat = cats.get("category", [])
            if isinstance(cat, list):
                for c in cat:
                    if c.get("classification") == "programme":
                        return c.get("title", "")
            elif isinstance(cat, dict):
                if cat.get("classification") == "programme":
                    return cat.get("title", "")
    return ""


# ── NSF Awards ──────────────────────────────────────────────────────────

NSF_URL = "https://api.nsf.gov/services/v1/awards.json"

NSF_QUERIES = [
    "freshwater ecology",
    "aquatic biodiversity",
    "lake limnology",
    "river ecosystem",
    "fish ecology conservation",
    "environmental DNA water",
    "water quality",
    "plankton microbial aquatic",
]

NSF_FIELDS = (
    "id,title,fundsObligatedAmt,startDate,expDate,"
    "piFirstName,piLastName,abstractText,awardeeName,"
    "awardeeCity,fundProgramName"
)


def fetch_nsf_awards(max_per_query: int = 10, start_year: int = 2020) -> list:
    """Fetch freshwater-related NSF awards."""
    awards = []
    seen_ids = set()

    for query in NSF_QUERIES:
        try:
            resp = requests.get(
                NSF_URL,
                params={
                    "keyword": query,
                    "printFields": NSF_FIELDS,
                    "rpp": max_per_query,
                    "offset": 1,
                    "startDateStart": f"01/01/{start_year}",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            data = resp.json()
            for award in data.get("response", {}).get("award", []):
                aid = award.get("id", "")
                if not aid or aid in seen_ids:
                    continue
                seen_ids.add(aid)

                pi_name = f"{award.get('piFirstName', '')} {award.get('piLastName', '')}".strip()

                awards.append({
                    "source": "nsf",
                    "project_id": f"nsf-{aid}",
                    "title": award.get("title", ""),
                    "acronym": "",
                    "description": (award.get("abstractText") or "")[:500],
                    "funder": "NSF",
                    "programme": award.get("fundProgramName", ""),
                    "total_cost": award.get("fundsObligatedAmt", ""),
                    "start_date": _nsf_date(award.get("startDate", "")),
                    "end_date": _nsf_date(award.get("expDate", "")),
                    "status": "active" if award.get("expDate") else "",
                    "coordinator": f"{pi_name}, {award.get('awardeeName', '')}".strip(", "),
                    "url": f"https://www.nsf.gov/awardsearch/showAward?AWD_ID={aid}",
                    "keywords": "",
                })

            time.sleep(0.3)
        except Exception as e:
            log.warning(f"NSF fetch error for '{query}': {e}")
            continue

    return awards


def _nsf_date(date_str: str) -> str:
    """Convert NSF date format (mm/dd/yyyy) to ISO format."""
    if not date_str:
        return ""
    parts = date_str.split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    return date_str


# ── OpenAlex Funder-linked Papers ───────────────────────────────────────

OPENALEX_URL = "https://api.openalex.org/works"

# Major freshwater/environmental funders in OpenAlex
OPENALEX_FUNDERS = {
    "F4320332161": "Deutsche Forschungsgemeinschaft (DFG)",
    "F4320334678": "European Research Council (ERC)",
    "F4320332785": "National Science Foundation (NSF)",
    "F4320337790": "Horizon 2020",
}


def fetch_openalex_funded_papers(per_funder: int = 20) -> list:
    """Fetch recent freshwater-related papers with known funding sources."""
    papers = []
    seen = set()

    for funder_id, funder_name in OPENALEX_FUNDERS.items():
        try:
            resp = requests.get(
                OPENALEX_URL,
                params={
                    "filter": (
                        f"funders.id:{funder_id},"
                        "concepts.id:C18903297|C39432304|C159654592|C127413603,"  # freshwater, ecology, limnology, fish
                        "from_publication_date:2024-01-01"
                    ),
                    "per_page": per_funder,
                    "sort": "publication_date:desc",
                    "select": "id,title,doi,publication_date,funders,authorships",
                    "mailto": "scopefish@igb-berlin.de",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            for work in resp.json().get("results", []):
                wid = work.get("id", "")
                if wid in seen:
                    continue
                seen.add(wid)

                # Extract first author + institution
                authors = work.get("authorships", [])
                pi = ""
                if authors:
                    a = authors[0]
                    pi = a.get("author", {}).get("display_name", "")
                    insts = a.get("institutions", [])
                    if insts:
                        pi += f", {insts[0].get('display_name', '')}"

                papers.append({
                    "source": "openalex",
                    "project_id": wid,
                    "title": work.get("title", ""),
                    "acronym": "",
                    "description": "",
                    "funder": funder_name,
                    "programme": "",
                    "total_cost": "",
                    "start_date": work.get("publication_date", ""),
                    "end_date": "",
                    "status": "published",
                    "coordinator": pi,
                    "url": work.get("doi") or "",
                    "keywords": "",
                })

            time.sleep(0.2)
        except Exception as e:
            log.warning(f"OpenAlex funder fetch error for {funder_name}: {e}")
            continue

    return papers


# ── EU Open Calls (SEDIA API) ──────────────────────────────────────────

SEDIA_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

SEDIA_QUERIES = [
    "freshwater biodiversity",
    "aquatic ecosystem",
    "water resources environmental",
    "fish ecology conservation",
]


def fetch_eu_open_calls(max_results: int = 20) -> list:
    """Fetch open EU funding calls from the SEDIA API."""
    calls = []
    seen = set()

    for query in SEDIA_QUERIES:
        try:
            resp = requests.post(
                SEDIA_URL,
                params={
                    "apiKey": "SEDIA",
                    "text": query,
                    "pageSize": 10,
                    "query": "(type='callForProposal')",
                },
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            data = resp.json()
            for result in data.get("results", []):
                ref = result.get("reference", "")
                if not ref or ref in seen:
                    continue
                seen.add(ref)

                meta = result.get("metadata", {})
                title = _sedia_field(meta, "title")
                deadline = _sedia_field(meta, "deadlineDate")
                status = _sedia_field(meta, "status")

                calls.append({
                    "id": f"eu-{ref}",
                    "title": title or result.get("summary", ref),
                    "funder": "European Commission",
                    "description": result.get("summary", "")[:300],
                    "url": result.get("url", ""),
                    "deadline": deadline,
                    "amount": "",
                    "keywords": query,
                    "region": "EU",
                })

            time.sleep(0.3)
        except Exception as e:
            log.warning(f"SEDIA fetch error for '{query}': {e}")
            continue

    return calls[:max_results]


def _sedia_field(meta: dict, field: str) -> str:
    val = meta.get(field, [])
    if isinstance(val, list) and val:
        return str(val[0])
    return str(val) if val else ""


# ── Aggregate fetcher ───────────────────────────────────────────────────

def fetch_all_projects(since_year: int = 2022) -> dict:
    """Fetch funded projects from all sources."""
    log.info("Fetching funded projects...")

    cordis = fetch_cordis_projects(max_per_query=10, since_year=since_year)
    log.info(f"  CORDIS: {len(cordis)} projects")

    nsf = fetch_nsf_awards(max_per_query=10, start_year=since_year)
    log.info(f"  NSF: {len(nsf)} awards")

    openalex = fetch_openalex_funded_papers(per_funder=15)
    log.info(f"  OpenAlex funded papers: {len(openalex)}")

    eu_calls = fetch_eu_open_calls(max_results=20)
    log.info(f"  EU open calls: {len(eu_calls)}")

    return {
        "projects": cordis + nsf + openalex,
        "calls": eu_calls,
    }
