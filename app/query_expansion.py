"""Query expansion for common scientific abbreviations in freshwater/ecology research."""

import re

# Abbreviation -> full form mappings
# When we see the abbreviation in a query, we append the full form
ABBREVIATIONS = {
    "DOM": "dissolved organic matter",
    "DOC": "dissolved organic carbon",
    "POC": "particulate organic carbon",
    "POM": "particulate organic matter",
    "TOC": "total organic carbon",
    "TN": "total nitrogen",
    "TP": "total phosphorus",
    "SRP": "soluble reactive phosphorus",
    "DIN": "dissolved inorganic nitrogen",
    "DO": "dissolved oxygen",
    "BOD": "biochemical oxygen demand",
    "COD": "chemical oxygen demand",
    "FT-ICR-MS": "Fourier transform ion cyclotron resonance mass spectrometry",
    "FTICR-MS": "Fourier transform ion cyclotron resonance mass spectrometry",
    "FTICRMS": "Fourier transform ion cyclotron resonance mass spectrometry",
    "LC-MS": "liquid chromatography mass spectrometry",
    "GC-MS": "gas chromatography mass spectrometry",
    "NMR": "nuclear magnetic resonance",
    "UV": "ultraviolet",
    "PAR": "photosynthetically active radiation",
    "GPP": "gross primary production",
    "NPP": "net primary production",
    "NEP": "net ecosystem production",
    "eDNA": "environmental DNA",
    "qPCR": "quantitative polymerase chain reaction",
    "OTU": "operational taxonomic unit",
    "ASV": "amplicon sequence variant",
    "NMDS": "non-metric multidimensional scaling",
    "PCA": "principal component analysis",
    "RDA": "redundancy analysis",
    "CCA": "canonical correspondence analysis",
    "PERMANOVA": "permutational multivariate analysis of variance",
    "ANOVA": "analysis of variance",
    "GAM": "generalized additive model",
    "GLM": "generalized linear model",
    "GLMM": "generalized linear mixed model",
    "GHG": "greenhouse gas",
    "CO2": "carbon dioxide",
    "CH4": "methane",
    "N2O": "nitrous oxide",
    "SUVA": "specific ultraviolet absorbance",
    "EEM": "excitation emission matrix",
    "PARAFAC": "parallel factor analysis",
    "CDOM": "chromophoric dissolved organic matter",
    "fDOM": "fluorescent dissolved organic matter",
    "SPE": "solid phase extraction",
    "WFD": "Water Framework Directive",
    "LTER": "long-term ecological research",
    "SDG": "Sustainable Development Goal",
    "IPCC": "Intergovernmental Panel on Climate Change",
    "IUCN": "International Union for Conservation of Nature",
}


def expand_query(query: str) -> str:
    """Expand abbreviations in the query to improve search coverage.

    Returns the original query with expansions appended.
    """
    expansions = []
    query_upper = query.upper()

    for abbr, full in ABBREVIATIONS.items():
        # Check if the abbreviation appears as a word boundary match
        pattern = r'\b' + re.escape(abbr) + r'\b'
        if re.search(pattern, query, re.IGNORECASE):
            # Don't add if the full form is already in the query
            if full.lower() not in query.lower():
                expansions.append(full)

    if expansions:
        return query + " " + " ".join(expansions)
    return query
