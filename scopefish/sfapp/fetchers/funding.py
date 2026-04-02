"""Fetch relevant funding calls for freshwater/ecology research.

Sources:
- Curated list of major funders with RSS/API links
- OpenAlex funder data
- Manual additions

Since most funding portals don't have open APIs, we maintain a curated
set of known relevant calls and link to funder portals for browsing.
"""

import hashlib
from datetime import datetime


# Curated funding portal links relevant to IGB research
FUNDING_PORTALS = [
    {
        "funder": "DFG (German Research Foundation)",
        "url": "https://www.dfg.de/en/research-funding/funding-opportunities",
        "region": "Germany",
        "keywords": "ecology, freshwater, biodiversity, environmental science",
    },
    {
        "funder": "Horizon Europe",
        "url": "https://ec.europa.eu/info/funding-tenders/opportunities/portal",
        "region": "EU",
        "keywords": "biodiversity, water, climate, ecosystems",
    },
    {
        "funder": "BMBF (Federal Ministry of Education and Research)",
        "url": "https://www.bmbf.de/bmbf/en/research/research-funding.html",
        "region": "Germany",
        "keywords": "water research, sustainability, biodiversity",
    },
    {
        "funder": "Leibniz Association",
        "url": "https://www.leibniz-gemeinschaft.de/en/research/leibniz-competition",
        "region": "Germany",
        "keywords": "interdisciplinary, collaborative research",
    },
    {
        "funder": "NSF (National Science Foundation)",
        "url": "https://www.nsf.gov/funding/",
        "region": "USA",
        "keywords": "ecology, environmental biology, hydrology",
    },
    {
        "funder": "ERC (European Research Council)",
        "url": "https://erc.europa.eu/apply-grant",
        "region": "EU",
        "keywords": "frontier research, all disciplines",
    },
    {
        "funder": "NERC (Natural Environment Research Council)",
        "url": "https://www.ukri.org/councils/nerc/",
        "region": "UK",
        "keywords": "environmental science, freshwater, biodiversity",
    },
    {
        "funder": "VolkswagenStiftung",
        "url": "https://www.volkswagenstiftung.de/en/funding",
        "region": "Germany",
        "keywords": "interdisciplinary, innovation, sustainability",
    },
    {
        "funder": "BiodivERsA+",
        "url": "https://www.biodiversa.eu/calls/",
        "region": "EU",
        "keywords": "biodiversity, ecosystem services, conservation",
    },
    {
        "funder": "Water JPI",
        "url": "https://www.waterjpi.eu/joint-calls",
        "region": "EU",
        "keywords": "water resources, aquatic ecosystems, water quality",
    },
]


def get_funding_portals() -> list:
    """Return the curated list of funding portals with IDs."""
    portals = []
    for portal in FUNDING_PORTALS:
        portal_id = hashlib.md5(portal["url"].encode()).hexdigest()[:8]
        portals.append({
            "id": portal_id,
            "title": f"{portal['funder']} — Funding Portal",
            "funder": portal["funder"],
            "description": f"Browse open calls from {portal['funder']}. Relevant topics: {portal['keywords']}.",
            "url": portal["url"],
            "deadline": "",
            "amount": "",
            "keywords": portal["keywords"],
            "region": portal["region"],
        })
    return portals
