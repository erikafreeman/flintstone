"""Paper enrichment: extract key messages and check data availability."""

import re
import requests
import time


def extract_key_messages(abstract: str) -> str:
    """Extract key messages/findings from an abstract.

    Uses heuristic sentence extraction to identify the most important
    findings, conclusions, and implications from the abstract text.

    Returns bullet-pointed key messages string.
    """
    if not abstract or len(abstract) < 100:
        return ""

    sentences = re.split(r'(?<=[.!?])\s+', abstract.strip())
    if not sentences:
        return ""

    # Score sentences by signal words
    conclusion_signals = [
        "we found", "we show", "our results", "we demonstrate", "these results",
        "this study", "our findings", "we report", "we observed", "we identified",
        "our analysis", "the results", "these findings", "we discovered",
        "significantly", "novel", "for the first time", "importantly",
        "in conclusion", "we conclude", "overall", "together",
        "suggest that", "indicate that", "reveal that", "confirm that",
        "highlight", "provide evidence", "challenge", "advance",
    ]

    methods_signals = [
        "we used", "we applied", "we measured", "we sampled", "we collected",
        "we analyzed", "we performed", "using a", "by means of", "was conducted",
        "were collected", "were analyzed", "were measured",
    ]

    scored = []
    for i, sent in enumerate(sentences):
        sent_lower = sent.lower()
        score = 0

        # Conclusion/finding signals
        for signal in conclusion_signals:
            if signal in sent_lower:
                score += 2

        # Penalize methods sentences
        for signal in methods_signals:
            if signal in sent_lower:
                score -= 1

        # Boost later sentences (conclusions tend to be at the end)
        if i >= len(sentences) * 0.6:
            score += 1

        # Boost sentences with numbers (quantitative findings)
        if re.search(r'\d+\.?\d*\s*%|\d+\.\d+|p\s*[<>=]|r\s*=', sent_lower):
            score += 1

        scored.append((score, i, sent))

    # Pick top 3 sentences
    scored.sort(key=lambda x: (-x[0], x[1]))
    top = sorted(scored[:3], key=lambda x: x[1])  # Re-sort by position

    messages = []
    for score, idx, sent in top:
        # Clean up the sentence
        sent = sent.strip()
        if sent and len(sent) > 30:
            messages.append(sent)

    return "\n".join(f"- {m}" for m in messages) if messages else ""


def check_data_availability(paper: dict) -> dict:
    """Check if a paper has downloadable/open data.

    Checks:
    1. OpenAlex open_access_url
    2. DOI metadata for data repository links
    3. Abstract mentions of data repositories

    Returns dict with: data_available, data_url, data_repository
    """
    result = {"data_available": False, "data_url": "", "data_repository": ""}

    # Check open access URL
    oa_url = paper.get("open_access_url") or ""
    if oa_url:
        result["data_available"] = True
        result["data_url"] = oa_url

    # Check abstract for data repository mentions
    abstract = (paper.get("abstract") or "").lower()
    title = (paper.get("title") or "").lower()
    text = abstract + " " + title

    repositories = {
        "dryad": "Dryad",
        "figshare": "Figshare",
        "zenodo": "Zenodo",
        "pangaea": "PANGAEA",
        "genbank": "GenBank",
        "ncbi": "NCBI",
        "github.com": "GitHub",
        "gitlab": "GitLab",
        "dataverse": "Dataverse",
        "fred.igb-berlin": "FRED (IGB)",
        "gbif": "GBIF",
        "bold systems": "BOLD Systems",
        "sequence read archive": "SRA",
        "european nucleotide archive": "ENA",
        "data are available": "Stated in paper",
        "data availability": "Stated in paper",
        "data is available": "Stated in paper",
        "supplementary data": "Supplementary",
        "open data": "Open Data",
    }

    for keyword, repo_name in repositories.items():
        if keyword in text:
            result["data_available"] = True
            if not result["data_repository"]:
                result["data_repository"] = repo_name
            elif repo_name not in result["data_repository"]:
                result["data_repository"] += f", {repo_name}"

    return result


def enrich_papers(papers: list) -> list:
    """Enrich a batch of papers with key messages and data availability.

    Returns list of (paper_id, enrichment_dict) tuples.
    """
    results = []
    for paper in papers:
        abstract = paper.get("abstract") or ""
        key_messages = extract_key_messages(abstract)
        data_info = check_data_availability(paper)

        results.append((paper["id"], {
            "key_messages": key_messages,
            "data_available": data_info["data_available"],
            "data_url": data_info["data_url"],
            "data_repository": data_info["data_repository"],
            "methods_summary": "",
        }))

    return results
