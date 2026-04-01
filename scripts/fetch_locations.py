"""Fetch geographic locations from FRED and extract from IGB publication abstracts."""

import json
import os
import re
import sqlite3
import sys
import time
import requests

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "app", "static", "locations.json")

DATACITE_API = "https://api.datacite.org/dois"
FRED_PREFIX = "10.18728"

# Known IGB study sites with coordinates.
# Each entry maps a search term -> {lat, lon, type, canonical_name}.
# Multiple spellings (umlaut, ascii, ue/oe/ae) all point to the same site.
_SITES_RAW = [
    # === Berlin & Brandenburg lakes ===
    {"name": "Müggelsee", "lat": 52.438, "lon": 13.653, "type": "lake",
     "aliases": ["muggelsee", "mueggelsee", "müggelsee", "lake müggelsee", "großer müggelsee", "grosser muggelsee"]},
    {"name": "Lake Stechlin", "lat": 53.151, "lon": 13.030, "type": "lake",
     "aliases": ["stechlin", "stechlinsee", "lake stechlin", "großer stechlinsee", "lakelab"]},
    {"name": "Arendsee", "lat": 52.889, "lon": 11.476, "type": "lake",
     "aliases": ["arendsee"]},
    {"name": "Wannsee", "lat": 52.43, "lon": 13.18, "type": "lake",
     "aliases": ["wannsee", "großer wannsee"]},
    {"name": "Tegeler See", "lat": 52.58, "lon": 13.26, "type": "lake",
     "aliases": ["tegeler see", "tegeler"]},
    {"name": "Schlachtensee", "lat": 52.448, "lon": 13.212, "type": "lake",
     "aliases": ["schlachtensee"]},
    {"name": "Plötzensee", "lat": 52.543, "lon": 13.342, "type": "lake",
     "aliases": ["plötzensee", "plotzensee", "ploetzensee"]},
    {"name": "Groß Glienicker See", "lat": 52.46, "lon": 13.11, "type": "lake",
     "aliases": ["groß glienicker", "gross glienicker"]},
    {"name": "Scharmützelsee", "lat": 52.23, "lon": 14.04, "type": "lake",
     "aliases": ["scharmützelsee", "scharmuetzelsee", "scharmutzel"]},
    {"name": "Grimnitzsee", "lat": 52.91, "lon": 13.78, "type": "lake",
     "aliases": ["grimnitzsee"]},
    {"name": "Haussee", "lat": 53.02, "lon": 13.65, "type": "lake",
     "aliases": ["haussee"]},
    {"name": "Großer Vätersee", "lat": 53.00, "lon": 13.63, "type": "lake",
     "aliases": ["großer vätersee", "grosser vaetersee", "vätersee"]},
    {"name": "Dagowsee", "lat": 53.14, "lon": 13.05, "type": "lake",
     "aliases": ["dagowsee"]},
    {"name": "Döllnsee", "lat": 52.99, "lon": 13.58, "type": "lake",
     "aliases": ["döllnsee", "doellnsee", "dollnsee"]},
    {"name": "Breiter Luzin", "lat": 53.38, "lon": 13.46, "type": "lake",
     "aliases": ["breiter luzin"]},
    {"name": "Feldberger Seen", "lat": 53.34, "lon": 13.44, "type": "lake",
     "aliases": ["feldberger"]},

    # === Other German lakes ===
    {"name": "Bodensee", "lat": 47.633, "lon": 9.375, "type": "lake",
     "aliases": ["bodensee", "lake constance"]},
    {"name": "Müritz", "lat": 53.43, "lon": 12.7, "type": "lake",
     "aliases": ["müritz", "mueritz", "muritz"]},
    {"name": "Plauer See", "lat": 53.44, "lon": 12.3, "type": "lake",
     "aliases": ["plauer see"]},
    {"name": "Chiemsee", "lat": 47.86, "lon": 12.4, "type": "lake",
     "aliases": ["chiemsee"]},
    {"name": "Ammersee", "lat": 47.99, "lon": 11.12, "type": "lake",
     "aliases": ["ammersee"]},
    {"name": "Starnberger See", "lat": 47.9, "lon": 11.31, "type": "lake",
     "aliases": ["starnberger see"]},
    {"name": "Steinhuder Meer", "lat": 52.46, "lon": 9.33, "type": "lake",
     "aliases": ["steinhuder meer"]},
    {"name": "Neusiedler See", "lat": 47.77, "lon": 16.77, "type": "lake",
     "aliases": ["neusiedler see"]},
    {"name": "Bautzen Reservoir", "lat": 51.18, "lon": 14.44, "type": "lake",
     "aliases": ["bautzen"]},

    # === German/European rivers ===
    {"name": "Spree", "lat": 52.497, "lon": 13.455, "type": "river",
     "aliases": ["spree", "spreewald"]},
    {"name": "Havel", "lat": 52.521, "lon": 13.176, "type": "river",
     "aliases": ["havel"]},
    {"name": "Dahme", "lat": 52.426, "lon": 13.562, "type": "river",
     "aliases": ["dahme"]},
    {"name": "Löcknitz", "lat": 52.45, "lon": 13.72, "type": "river",
     "aliases": ["löcknitz", "locknitz", "loecknitz"]},
    {"name": "Elbe", "lat": 53.545, "lon": 9.96, "type": "river",
     "aliases": ["elbe"]},
    {"name": "Rhine", "lat": 50.36, "lon": 7.6, "type": "river",
     "aliases": ["rhine", "rhein"]},
    {"name": "Oder", "lat": 52.75, "lon": 14.55, "type": "river",
     "aliases": ["oder"]},
    {"name": "Danube", "lat": 48.22, "lon": 16.38, "type": "river",
     "aliases": ["danube", "donau"]},
    {"name": "Warnow", "lat": 54.09, "lon": 12.13, "type": "river",
     "aliases": ["warnow"]},
    {"name": "Peene", "lat": 53.85, "lon": 13.07, "type": "river",
     "aliases": ["peene"]},
    {"name": "Tagliamento", "lat": 46.15, "lon": 12.97, "type": "river",
     "aliases": ["tagliamento"]},

    # === Baltic/North/Coastal ===
    {"name": "Baltic Sea", "lat": 57.0, "lon": 18.0, "type": "marine",
     "aliases": ["baltic sea", "baltic"]},
    {"name": "North Sea", "lat": 56.0, "lon": 4.0, "type": "marine",
     "aliases": ["north sea"]},
    {"name": "Mediterranean", "lat": 38.0, "lon": 18.0, "type": "marine",
     "aliases": ["mediterranean"]},
    {"name": "Bodden", "lat": 54.4, "lon": 13.1, "type": "marine",
     "aliases": ["bodden"]},
    {"name": "Rügen", "lat": 54.43, "lon": 13.43, "type": "marine",
     "aliases": ["rügen", "ruegen", "rugen"]},

    # === International lakes ===
    {"name": "Lake Erken", "lat": 59.842, "lon": 18.565, "type": "lake",
     "aliases": ["erken", "lake erken"]},
    {"name": "Lake Balaton", "lat": 46.833, "lon": 17.733, "type": "lake",
     "aliases": ["lake balaton", "balaton"]},
    {"name": "Lake Geneva", "lat": 46.45, "lon": 6.55, "type": "lake",
     "aliases": ["lake geneva", "lac léman", "lac leman"]},
    {"name": "Lake Lugano", "lat": 46.0, "lon": 9.0, "type": "lake",
     "aliases": ["lake lugano"]},
    {"name": "Lake Taihu", "lat": 31.2, "lon": 120.2, "type": "lake",
     "aliases": ["lake taihu", "taihu"]},
    {"name": "Lake Baikal", "lat": 53.5, "lon": 108.0, "type": "lake",
     "aliases": ["lake baikal", "baikal"]},
    {"name": "Lake Kivu", "lat": -2.05, "lon": 29.0, "type": "lake",
     "aliases": ["lake kivu", "kivu"]},
    {"name": "Lake Tanganyika", "lat": -6.0, "lon": 29.5, "type": "lake",
     "aliases": ["lake tanganyika", "tanganyika"]},
    {"name": "Lake Victoria", "lat": -1.0, "lon": 33.0, "type": "lake",
     "aliases": ["lake victoria"]},
    {"name": "Lake Chad", "lat": 13.0, "lon": 14.0, "type": "lake",
     "aliases": ["lake chad"]},
    {"name": "Lake Malawi", "lat": -12.0, "lon": 34.5, "type": "lake",
     "aliases": ["lake malawi"]},
    {"name": "Lusatian Lakes", "lat": 51.5, "lon": 14.2, "type": "lake",
     "aliases": ["lusatia", "lausitz", "lusatian"]},

    # === International rivers ===
    {"name": "Mekong", "lat": 15.0, "lon": 105.0, "type": "river",
     "aliases": ["mekong"]},
    {"name": "Amazon", "lat": -3.0, "lon": -60.0, "type": "river",
     "aliases": ["amazon"]},
    {"name": "Nile", "lat": 26.0, "lon": 32.0, "type": "river",
     "aliases": ["nile"]},
    {"name": "Yangtze", "lat": 31.0, "lon": 117.0, "type": "river",
     "aliases": ["yangtze"]},
]

# Build lookup: alias -> site info
KNOWN_SITES = {}
for site in _SITES_RAW:
    info = {"lat": site["lat"], "lon": site["lon"], "type": site["type"], "name": site["name"]}
    for alias in site["aliases"]:
        KNOWN_SITES[alias.lower()] = info


def fetch_fred_locations():
    """Fetch dataset locations from FRED via DataCite API."""
    locations = []
    page = 1
    page_size = 100

    print("Fetching FRED dataset locations from DataCite...")

    while True:
        url = f"{DATACITE_API}?query=prefix:{FRED_PREFIX}&page[size]={page_size}&page[number]={page}"
        resp = requests.get(url, timeout=15)

        if resp.status_code == 429:
            time.sleep(5)
            continue
        resp.raise_for_status()
        data = resp.json()

        items = data.get("data", [])
        if not items:
            break

        for item in items:
            attrs = item.get("attributes", {})
            title = attrs.get("titles", [{}])[0].get("title", "")
            doi = attrs.get("doi", "")
            year = (attrs.get("publicationYear") or "")

            # Extract geographic locations
            geo_locs = attrs.get("geoLocations", [])
            for geo in geo_locs:
                point = geo.get("geoLocationPoint", {})
                lat = point.get("pointLatitude")
                lon = point.get("pointLongitude")
                place = geo.get("geoLocationPlace", "")

                if lat and lon:
                    locations.append({
                        "lat": float(lat),
                        "lon": float(lon),
                        "title": title,
                        "doi": f"https://doi.org/{doi}" if doi else "",
                        "source": "FRED",
                        "place": place,
                        "year": year,
                        "type": "dataset",
                    })

        total_pages = data.get("meta", {}).get("totalPages", 1)
        print(f"  Page {page}/{total_pages}: {len(items)} datasets")

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)

    return locations


def extract_locations_from_abstracts(conn):
    """Extract known site names from publication abstracts."""
    cur = conn.cursor()
    cur.execute("SELECT id, title, abstract, year, doi FROM publications WHERE abstract != ''")
    rows = cur.fetchall()

    locations = []
    site_pub_count = {}  # keyed by canonical name

    for pub_id, title, abstract, year, doi in rows:
        text = f"{title or ''} {abstract or ''}".lower()

        # Track which canonical sites were already matched for this pub (avoid double-counting)
        matched_sites = set()

        for alias, info in KNOWN_SITES.items():
            if alias in text:
                canonical = info["name"]
                if canonical in matched_sites:
                    continue
                matched_sites.add(canonical)

                if canonical not in site_pub_count:
                    site_pub_count[canonical] = {
                        "lat": info["lat"],
                        "lon": info["lon"],
                        "site": canonical,
                        "type": info["type"],
                        "publications": [],
                    }
                site_pub_count[canonical]["publications"].append({
                    "title": title,
                    "year": year,
                    "doi": doi or "",
                })

    # Convert to location list
    for key, data in site_pub_count.items():
        pub_count = len(data["publications"])
        # Pick the 3 most recent publications for display
        top_pubs = sorted(data["publications"], key=lambda p: p.get("year") or 0, reverse=True)[:3]
        locations.append({
            "lat": data["lat"],
            "lon": data["lon"],
            "title": data["site"],
            "source": "publications",
            "type": data["type"],
            "pub_count": pub_count,
            "top_pubs": top_pubs,
        })

    return locations


def main():
    conn = sqlite3.connect(DB_PATH)

    # 1. FRED datasets
    fred_locations = fetch_fred_locations()
    print(f"Found {len(fred_locations)} FRED dataset locations")

    # 2. Publication abstracts
    print("\nExtracting locations from publication abstracts...")
    pub_locations = extract_locations_from_abstracts(conn)
    print(f"Found {len(pub_locations)} study sites mentioned in publications")

    for loc in sorted(pub_locations, key=lambda l: -l["pub_count"])[:10]:
        print(f"  {loc['title']}: {loc['pub_count']} publications")

    conn.close()

    # Combine and save
    all_locations = fred_locations + pub_locations

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_locations, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(all_locations)} locations to {OUTPUT_PATH}")
    print(f"  FRED datasets: {len(fred_locations)}")
    print(f"  Publication sites: {len(pub_locations)}")


if __name__ == "__main__":
    main()
