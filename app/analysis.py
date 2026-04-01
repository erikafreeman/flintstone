"""Collaborator ranking and novelty detection with TF-IDF."""

import math
import re
from collections import Counter


STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "be",
    "been", "being", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "shall", "can", "this",
    "that", "these", "those", "it", "its", "we", "our", "they", "their",
    "which", "what", "who", "how", "when", "where", "not", "no", "also",
    "than", "more", "most", "such", "very", "both", "each", "all", "any",
    "between", "through", "about", "into", "over", "after", "before",
    "using", "based", "across", "within", "among", "along", "including",
    "specific", "specifically", "approach", "study", "studied", "studies",
    "project", "investigates", "investigate", "research", "understanding",
    "term", "well", "new", "novel", "however", "thus", "therefore",
    "results", "show", "showed", "found", "observed", "used", "use",
    "different", "significant", "significantly", "effect", "effects",
    "high", "higher", "low", "lower", "large", "small", "important",
    "first", "two", "three", "one", "here", "often", "many", "several",
    "abstract", "during", "since", "while", "although", "whether",
}


def _tokenize(text: str) -> list:
    """Extract meaningful words from text."""
    words = re.findall(r'\b[a-z][a-z-]{2,}\b', text.lower())
    return [w for w in words if w not in STOP_WORDS]


def _extract_ngrams(tokens: list, n: int = 2) -> list:
    """Extract n-grams from a token list."""
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def rank_collaborators(pub_ids: list, authors_by_pub: dict, publications: list) -> list:
    """Rank IGB authors by frequency in top results."""
    author_counter = Counter()
    author_info = {}
    author_papers = {}

    pub_map = {p["id"]: p for p in publications}

    for pub_id in pub_ids:
        igb_authors = authors_by_pub.get(pub_id, [])
        for author in igb_authors:
            aid = author["id"]
            author_counter[aid] += 1
            author_info[aid] = {
                "id": aid,
                "display_name": author["display_name"],
                "orcid": author.get("orcid", ""),
                "department": author.get("department", ""),
                "is_current_staff": author.get("is_current_staff", 0),
            }
            if aid not in author_papers:
                author_papers[aid] = []
            pub = pub_map.get(pub_id)
            if pub and len(author_papers[aid]) < 3:
                author_papers[aid].append({
                    "title": pub["title"],
                    "year": pub["year"],
                    "doi": pub["doi"],
                })

    ranked = []
    for aid, count in author_counter.most_common(20):
        info = author_info[aid]
        info["relevant_count"] = count
        info["top_papers"] = author_papers.get(aid, [])
        ranked.append(info)

    return ranked


def detect_novelty(query: str, publications: list) -> dict:
    """Identify novel aspects using TF-IDF weighting.

    Computes term importance in the query relative to the corpus of
    top matching publications, highlighting terms that are important
    to the query but rare/absent in IGB's existing work.
    """
    query_tokens = _tokenize(query)
    query_bigrams = _extract_ngrams(query_tokens)

    # Build per-document token sets for IDF calculation
    doc_tokens = []
    for pub in publications:
        text = f"{pub.get('title', '')} {pub.get('abstract', '')}"
        doc_tokens.append(set(_tokenize(text)))

    n_docs = len(doc_tokens) + 1  # +1 for the query itself

    # Compute TF for query terms
    query_tf = Counter(query_tokens)
    max_tf = max(query_tf.values()) if query_tf else 1

    # Compute IDF across the corpus
    def idf(term):
        doc_count = sum(1 for doc in doc_tokens if term in doc)
        return math.log((n_docs + 1) / (doc_count + 1)) + 1

    # TF-IDF scores for query terms
    tfidf_scores = {}
    for term, count in query_tf.items():
        tf = 0.5 + 0.5 * (count / max_tf)  # augmented TF
        tfidf_scores[term] = tf * idf(term)

    # Sort terms by TF-IDF score
    sorted_terms = sorted(tfidf_scores.items(), key=lambda x: -x[1])

    # Classify: terms with high IDF (rare in corpus) are novel
    corpus_terms = set()
    for doc in doc_tokens:
        corpus_terms.update(doc)

    novel_terms = []
    covered_terms = []
    for term, score in sorted_terms:
        if term in corpus_terms:
            covered_terms.append((term, score))
        else:
            novel_terms.append((term, score))

    # Bigram analysis
    corpus_text = " ".join(
        f"{p.get('title', '')} {p.get('abstract', '')}" for p in publications
    )
    corpus_bigrams = set(_extract_ngrams(_tokenize(corpus_text)))
    novel_bigrams = [bg for bg in query_bigrams if bg not in corpus_bigrams]
    covered_bigrams = [bg for bg in query_bigrams if bg in corpus_bigrams]

    # Build smart summary
    summary_parts = []

    # Find the top covered themes (high TF-IDF but present)
    top_covered = [t for t, s in covered_terms[:5]]
    if top_covered:
        summary_parts.append(
            f"Your idea connects strongly with existing IGB work on "
            f"**{', '.join(top_covered[:3])}**."
        )

    # Highlight the most distinctive novel terms (highest TF-IDF and absent)
    top_novel = [t for t, s in novel_terms[:5]]
    if top_novel:
        summary_parts.append(
            f"The emphasis on **{', '.join(top_novel[:3])}** appears to be "
            f"a distinctive contribution not well represented in IGB's publication record."
        )

    if novel_bigrams:
        top_novel_bi = novel_bigrams[:3]
        summary_parts.append(
            f"The combinations **{', '.join(top_novel_bi)}** "
            f"represent potentially novel research angles."
        )

    if not novel_terms and not novel_bigrams:
        summary_parts.append(
            "Your query closely aligns with existing IGB research. "
            "Consider refining the angle or methodology to differentiate."
        )

    # Coverage ratio
    total_query_terms = len(set(query_tokens))
    covered_count = len([t for t, s in covered_terms])
    if total_query_terms > 0:
        pct = int(100 * covered_count / total_query_terms)
        summary_parts.append(
            f"Overall, {pct}% of your key terms are already present in IGB's work "
            f"({covered_count} of {total_query_terms} terms)."
        )

    return {
        "query_terms": [t for t, s in sorted_terms],
        "covered_terms": [t for t, s in covered_terms[:15]],
        "novel_terms": [t for t, s in novel_terms[:15]],
        "novel_bigrams": novel_bigrams[:10],
        "covered_bigrams": covered_bigrams[:10],
        "summary": " ".join(summary_parts),
    }
