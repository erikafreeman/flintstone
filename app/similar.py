"""Find semantically similar papers using embedding cosine similarity."""

import numpy as np
from . import search


def find_similar(pub_id: str, top_k: int = 20) -> list:
    """Find papers most similar to a given publication.

    Returns list of (pub_id, similarity_score).
    """
    search.load()

    # Find the index of this publication in the embeddings
    idx = None
    for i, pid in enumerate(search._pub_ids):
        if pid == pub_id:
            idx = i
            break

    if idx is None:
        return []

    # Get this paper's embedding
    query_emb = search._embeddings[idx:idx+1]  # shape (1, dim)

    # Cosine similarity against all papers
    similarities = np.dot(search._embeddings, query_emb.T).flatten()

    # Also check chunk embeddings for richer similarity
    chunk_boost = {}
    if search._chunk_embeddings is not None and search._chunk_pub_map:
        # Get chunks for this publication
        pub_chunk_indices = []
        for ci, cid in enumerate(search._chunk_ids):
            cid_int = int(cid)
            if search._chunk_pub_map.get(cid_int) == pub_id:
                pub_chunk_indices.append(ci)

        if pub_chunk_indices:
            # Average the chunk embeddings for this paper
            pub_chunk_embs = search._chunk_embeddings[pub_chunk_indices]
            avg_chunk_emb = np.mean(pub_chunk_embs, axis=0, keepdims=True)
            avg_chunk_emb = avg_chunk_emb / np.linalg.norm(avg_chunk_emb)

            # Find similar chunks
            chunk_sims = np.dot(search._chunk_embeddings, avg_chunk_emb.T).flatten()
            top_chunk_indices = np.argsort(chunk_sims)[::-1][:200]

            for ci in top_chunk_indices:
                cid_int = int(search._chunk_ids[ci])
                other_pub = search._chunk_pub_map.get(cid_int)
                if other_pub and other_pub != pub_id:
                    if other_pub not in chunk_boost:
                        chunk_boost[other_pub] = float(chunk_sims[ci])

    # Sort by similarity, exclude self and review artifacts
    top_indices = np.argsort(similarities)[::-1]

    results = []
    seen_titles = set()
    for i in top_indices:
        pid = search._pub_ids[i]
        if pid == pub_id:
            continue
        if search._is_review_artifact(pid):
            continue

        meta = search._pub_meta.get(pid)
        if not meta:
            continue

        # Dedup by title
        title_key = meta["title_lower"]
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        # Combine abstract similarity with chunk similarity
        sim = float(similarities[i])
        if pid in chunk_boost:
            sim = 0.7 * sim + 0.3 * chunk_boost[pid]

        results.append((pid, sim))

        if len(results) >= top_k:
            break

    # Re-sort by combined score
    results.sort(key=lambda x: -x[1])
    return results
