"""RSS feed parser for live funding calls and press releases."""

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

log = logging.getLogger(__name__)

# ── Funding RSS Feeds ───────────────────────────────────────────────────

FUNDING_FEEDS = [
    {
        "name": "DFG Funding Opportunities",
        "url": "https://www.dfg.de/en/research-funding/announcements/rss-feed/index.xml",
        "funder": "DFG",
        "region": "Germany",
    },
    {
        "name": "BMBF Announcements",
        "url": "https://www.bmbf.de/SiteGlobals/Functions/RSSFeed/de/RSSNewsfeed/Bekanntmachungen/RSSNewsfeed_Bekanntmachungen.xml",
        "funder": "BMBF",
        "region": "Germany",
    },
    {
        "name": "NSF Environmental Biology",
        "url": "https://www.nsf.gov/rss/rss_www_funding_pgm_annc_702.xml",
        "funder": "NSF",
        "region": "USA",
    },
    {
        "name": "NERC Opportunities",
        "url": "https://www.ukri.org/opportunity/?filter_council%5B%5D=NERC&feed=rss2",
        "funder": "NERC",
        "region": "UK",
    },
]

# Keywords to filter for freshwater/environmental relevance
RELEVANCE_KEYWORDS = [
    "water", "freshwater", "aquatic", "lake", "river", "fish", "biodiversity",
    "ecology", "ecosystem", "environment", "climate", "conservation", "marine",
    "hydrology", "limnology", "wetland", "pollution", "sustainability",
    "species", "habitat", "restoration", "monitoring", "genomic", "eDNA",
]


def fetch_funding_rss() -> list:
    """Fetch and parse funding call RSS feeds."""
    calls = []

    for feed_info in FUNDING_FEEDS:
        try:
            resp = requests.get(
                feed_info["url"],
                headers={"User-Agent": "Scopefish/1.0 (IGB research tool)"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.warning(f"RSS feed {feed_info['name']} returned {resp.status_code}")
                continue

            items = _parse_rss(resp.text)
            for item in items:
                # Filter for relevance
                text = f"{item.get('title', '')} {item.get('description', '')}".lower()
                if not any(kw in text for kw in RELEVANCE_KEYWORDS):
                    continue

                calls.append({
                    "id": f"rss-{feed_info['funder'].lower()}-{_slug(item.get('title', ''))}",
                    "title": item.get("title", ""),
                    "funder": feed_info["funder"],
                    "description": _clean_html(item.get("description", ""))[:300],
                    "url": item.get("link", ""),
                    "deadline": item.get("deadline", ""),
                    "amount": "",
                    "keywords": ", ".join(kw for kw in RELEVANCE_KEYWORDS if kw in text)[:200],
                    "region": feed_info["region"],
                })

            time.sleep(0.2)
        except Exception as e:
            log.warning(f"RSS fetch error for {feed_info['name']}: {e}")
            continue

    return calls


def _parse_rss(xml_text: str) -> list:
    """Parse RSS/Atom feed XML into a list of items."""
    items = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    # Handle RSS 2.0
    for item in root.iter("item"):
        items.append({
            "title": _elem_text(item, "title"),
            "link": _elem_text(item, "link"),
            "description": _elem_text(item, "description"),
            "pub_date": _elem_text(item, "pubDate"),
            "deadline": "",
        })

    # Handle Atom
    if not items:
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall(".//atom:entry", ns):
            link = ""
            link_el = entry.find("atom:link", ns)
            if link_el is not None:
                link = link_el.get("href", "")
            items.append({
                "title": _elem_text(entry, "atom:title", ns),
                "link": link,
                "description": _elem_text(entry, "atom:summary", ns) or _elem_text(entry, "atom:content", ns),
                "pub_date": _elem_text(entry, "atom:published", ns) or _elem_text(entry, "atom:updated", ns),
                "deadline": "",
            })

    return items


def _elem_text(parent, tag, ns=None):
    if ns:
        el = parent.find(tag, ns)
    else:
        el = parent.find(tag)
    return el.text.strip() if el is not None and el.text else ""


# ── Press Release Feeds ─────────────────────────────────────────────────

PRESS_FEEDS = [
    {
        "name": "ScienceDaily Environment",
        "url": "https://www.sciencedaily.com/rss/earth_climate/freshwater.xml",
        "source": "ScienceDaily",
    },
    {
        "name": "ScienceDaily Biology",
        "url": "https://www.sciencedaily.com/rss/plants_animals/fish.xml",
        "source": "ScienceDaily",
    },
    {
        "name": "Phys.org Earth Sciences",
        "url": "https://phys.org/rss-feed/earth-news/ecology/",
        "source": "Phys.org",
    },
]


def fetch_press_releases() -> list:
    """Fetch science press releases from RSS feeds."""
    releases = []

    for feed_info in PRESS_FEEDS:
        try:
            resp = requests.get(
                feed_info["url"],
                headers={"User-Agent": "Scopefish/1.0 (IGB research tool)"},
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            items = _parse_rss(resp.text)
            for item in items[:10]:  # Top 10 per feed
                text = f"{item.get('title', '')} {item.get('description', '')}".lower()
                if not any(kw in text for kw in RELEVANCE_KEYWORDS):
                    continue

                releases.append({
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "description": _clean_html(item.get("description", ""))[:200],
                    "source": feed_info["source"],
                    "pub_date": item.get("pub_date", ""),
                })

        except Exception as e:
            log.warning(f"Press RSS error for {feed_info['name']}: {e}")
            continue

    return releases


def _clean_html(text: str) -> str:
    """Remove HTML tags from text."""
    return re.sub(r"<[^>]+>", "", text).strip()


def _slug(text: str) -> str:
    """Create a URL-safe slug from text."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower().strip())[:50]
