"""Embedding-based semantic search with hybrid scoring, query expansion, and re-ranking."""

import os
import sqlite3
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

from .query_expansion import expand_query

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DB_PATH = os.path.join(DATA_DIR, "flinstone.db")

# Embedding model (all-mpnet-base-v2: 768-dim, much better than MiniLM)
EMBED_MODEL_DIR = os.path.join(DATA_DIR, "model_specter2")
EMBED_ONNX = os.path.join(EMBED_MODEL_DIR, "onnx", "model.onnx")
EMBED_TOK = os.path.join(EMBED_MODEL_DIR, "tokenizer.json")

# Fallback to original MiniLM model if mpnet not available
FALLBACK_MODEL_DIR = os.path.join(DATA_DIR, "model")
FALLBACK_ONNX = os.path.join(FALLBACK_MODEL_DIR, "onnx", "model.onnx")
FALLBACK_TOK = os.path.join(FALLBACK_MODEL_DIR, "tokenizer.json")

# Re-ranker model (cross-encoder)
RERANKER_DIR = os.path.join(DATA_DIR, "model_reranker")
RERANKER_ONNX = os.path.join(RERANKER_DIR, "onnx", "model.onnx")
RERANKER_TOK = os.path.join(RERANKER_DIR, "tokenizer.json")

# State
_embed_session = None
_embed_tokenizer = None
_reranker_session = None
_reranker_tokenizer = None
_embeddings = None
_pub_ids = None
_pub_meta = None  # {pub_id: {title_lower, year, type, cited_by_count, title, abstract}}
_using_mpnet = False

# Chunk search state
_chunk_embeddings = None
_chunk_ids = None      # numpy array of chunk DB IDs
_chunk_pub_map = None  # {chunk_db_id: publication_id}
_chunk_texts = None    # {chunk_db_id: chunk_text} (for re-ranker context)

# Peer review artifacts to filter
REVIEW_PREFIXES = (
    "reply on rc", "comment on ", "author comment", "referee comment",
    "short comment", "author response",
)


def _mean_pooling(token_embeddings, attention_mask):
    mask_expanded = np.expand_dims(attention_mask, axis=-1).astype(np.float32)
    sum_embeddings = np.sum(token_embeddings * mask_expanded, axis=1)
    sum_mask = np.clip(np.sum(mask_expanded, axis=1), a_min=1e-9, a_max=None)
    return sum_embeddings / sum_mask


def _normalize(embeddings):
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return embeddings / norms


def _encode(texts, max_length=384):
    """Encode texts using the embedding model."""
    encodings = [_embed_tokenizer.encode(text) for text in texts]
    max_len = min(max(len(e.ids) for e in encodings), max_length)

    input_ids, attention_mask = [], []
    token_type_ids = []
    for enc in encodings:
        ids = enc.ids[:max_len]
        mask = enc.attention_mask[:max_len]
        pad_len = max_len - len(ids)
        input_ids.append(ids + [0] * pad_len)
        attention_mask.append(mask + [0] * pad_len)
        if hasattr(enc, 'type_ids'):
            ttype = enc.type_ids[:max_len]
            token_type_ids.append(ttype + [0] * pad_len)

    input_ids = np.array(input_ids, dtype=np.int64)
    attention_mask_arr = np.array(attention_mask, dtype=np.int64)

    inputs = {"input_ids": input_ids, "attention_mask": attention_mask_arr}

    # MiniLM needs token_type_ids, mpnet doesn't
    if not _using_mpnet and token_type_ids:
        inputs["token_type_ids"] = np.array(token_type_ids, dtype=np.int64)

    outputs = _embed_session.run(None, inputs)
    embs = _mean_pooling(outputs[0], attention_mask_arr)
    return _normalize(embs)


def _rerank(query: str, candidates: list, top_n: int = 20) -> list:
    """Re-rank candidates using cross-encoder.

    candidates: list of (pub_id, score, title, abstract)
    Returns re-ordered list of (pub_id, new_score).
    """
    if not _reranker_session or not candidates:
        return [(c[0], c[1]) for c in candidates[:top_n]]

    pairs = []
    for pub_id, score, title, abstract in candidates[:min(50, len(candidates))]:
        doc_text = f"{title or ''} {(abstract or '')[:300]}"
        pairs.append((pub_id, score, doc_text))

    # Score each query-document pair
    reranker_scores = []
    batch_size = 8
    for i in range(0, len(pairs), batch_size):
        batch = pairs[i:i + batch_size]

        # Encode as [CLS] query [SEP] document [SEP]
        encodings = [_reranker_tokenizer.encode(query, b[2]) for b in batch]
        max_len = min(max(len(e.ids) for e in encodings), 512)

        input_ids, attention_mask, token_type_ids = [], [], []
        for enc in encodings:
            ids = enc.ids[:max_len]
            mask = enc.attention_mask[:max_len]
            ttype = enc.type_ids[:max_len]
            pad = max_len - len(ids)
            input_ids.append(ids + [0] * pad)
            attention_mask.append(mask + [0] * pad)
            token_type_ids.append(ttype + [0] * pad)

        outputs = _reranker_session.run(None, {
            "input_ids": np.array(input_ids, dtype=np.int64),
            "attention_mask": np.array(attention_mask, dtype=np.int64),
            "token_type_ids": np.array(token_type_ids, dtype=np.int64),
        })

        # Cross-encoder outputs logits — higher = more relevant
        logits = outputs[0]
        for j, (pub_id, orig_score, _) in enumerate(batch):
            ce_score = float(logits[j].flatten()[0])
            reranker_scores.append((pub_id, ce_score, orig_score))

    # Sort by cross-encoder score
    reranker_scores.sort(key=lambda x: -x[1])

    # Combine: use cross-encoder rank but keep the original semantic score for display
    result = []
    for pub_id, ce_score, orig_score in reranker_scores[:top_n]:
        result.append((pub_id, orig_score))

    return result


def load():
    global _embed_session, _embed_tokenizer, _embeddings, _pub_ids, _pub_meta
    global _reranker_session, _reranker_tokenizer, _using_mpnet
    if _embed_session is not None:
        return

    # Load embedding model — prefer mpnet, fallback to MiniLM
    if os.path.exists(os.path.join(DATA_DIR, "embeddings_mpnet.npy")) and os.path.exists(EMBED_ONNX):
        print("Loading all-mpnet-base-v2 model (768-dim)...")
        _embed_session = ort.InferenceSession(EMBED_ONNX)
        _embed_tokenizer = Tokenizer.from_file(EMBED_TOK)
        _embeddings = np.load(os.path.join(DATA_DIR, "embeddings_mpnet.npy"))
        _using_mpnet = True
    else:
        print("Loading all-MiniLM-L6-v2 model (384-dim, fallback)...")
        _embed_session = ort.InferenceSession(FALLBACK_ONNX)
        _embed_tokenizer = Tokenizer.from_file(FALLBACK_TOK)
        _embeddings = np.load(os.path.join(DATA_DIR, "embeddings.npy"))
        _using_mpnet = False

    _pub_ids = np.load(os.path.join(DATA_DIR, "pub_ids.npy"), allow_pickle=True)

    # Normalize
    norms = np.linalg.norm(_embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1
    _embeddings = _embeddings / norms

    # Load re-ranker
    if os.path.exists(RERANKER_ONNX):
        print("Loading cross-encoder re-ranker...")
        _reranker_session = ort.InferenceSession(RERANKER_ONNX)
        _reranker_tokenizer = Tokenizer.from_file(RERANKER_TOK)
    else:
        print("No re-ranker model found, skipping.")

    # Load metadata for filtering/dedup
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, title, year, type, cited_by_count, abstract FROM publications")
    _pub_meta = {}
    for row in cur.fetchall():
        _pub_meta[row[0]] = {
            "title_lower": (row[1] or "").lower().strip(),
            "title": row[1] or "",
            "year": row[2],
            "type": row[3] or "",
            "cited_by_count": row[4] or 0,
            "abstract": row[5] or "",
        }
    conn.close()

    # Load chunk embeddings if available
    global _chunk_embeddings, _chunk_ids, _chunk_pub_map, _chunk_texts
    chunk_emb_path = os.path.join(DATA_DIR, "chunk_embeddings.npy")
    chunk_ids_path = os.path.join(DATA_DIR, "chunk_ids.npy")

    if os.path.exists(chunk_emb_path) and os.path.exists(chunk_ids_path):
        _chunk_embeddings = np.load(chunk_emb_path)
        _chunk_ids = np.load(chunk_ids_path)
        # Normalize chunk embeddings
        cn = np.linalg.norm(_chunk_embeddings, axis=1, keepdims=True)
        cn[cn == 0] = 1
        _chunk_embeddings = _chunk_embeddings / cn

        # Build chunk_id -> publication_id map from DB
        _chunk_pub_map = {}
        _chunk_texts = {}
        try:
            cconn = sqlite3.connect(DB_PATH)
            for row in cconn.execute("SELECT id, publication_id, chunk_text FROM chunks"):
                _chunk_pub_map[row[0]] = row[1]
                _chunk_texts[row[0]] = row[2][:300]  # keep first 300 chars for re-ranker
            cconn.close()
        except Exception:
            _chunk_embeddings = None  # table doesn't exist yet

    n_chunks = len(_chunk_ids) if _chunk_ids is not None else 0
    print(f"Loaded {len(_pub_ids)} abstract embeddings ({_embeddings.shape[1]}-dim), "
          f"{n_chunks} chunk embeddings, "
          f"re-ranker: {'yes' if _reranker_session else 'no'}")


def _is_review_artifact(pub_id: str) -> bool:
    meta = _pub_meta.get(pub_id)
    if not meta:
        return False
    title = meta["title_lower"]
    if meta["type"] == "peer-review":
        return True
    return any(title.startswith(prefix) for prefix in REVIEW_PREFIXES)


def _semantic_search(query_emb, n_candidates):
    """Return ranked list by cosine similarity."""
    similarities = np.dot(_embeddings, query_emb.T).flatten()
    top_indices = np.argsort(similarities)[::-1][:n_candidates]
    return [(int(idx), float(similarities[idx])) for idx in top_indices]


def _chunk_search(query_emb, n_candidates=100):
    """Search chunk embeddings, return {pub_id: best_chunk_score}."""
    if _chunk_embeddings is None or len(_chunk_embeddings) == 0:
        return {}

    similarities = np.dot(_chunk_embeddings, query_emb.T).flatten()
    top_indices = np.argsort(similarities)[::-1][:n_candidates]

    pub_best = {}  # pub_id -> best chunk similarity
    for idx in top_indices:
        chunk_db_id = int(_chunk_ids[idx])
        pub_id = _chunk_pub_map.get(chunk_db_id)
        if pub_id and pub_id not in pub_best:
            pub_best[pub_id] = float(similarities[idx])

    return pub_best


def _fts_search(query: str, limit: int = 50) -> list:
    """Full-text keyword search, returns list of (pub_id, fts_rank)."""
    import re
    words = re.findall(r'[a-zA-Z0-9][\w-]{2,}', query)
    if not words:
        return []

    fts_query = " OR ".join(f'"{w}"' for w in words[:10])

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.execute(
            """
            SELECT id, rank FROM publications_fts
            WHERE publications_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (fts_query, limit),
        )
        results = [(row[0], float(row[1])) for row in cur.fetchall()]
        conn.close()
        return results
    except Exception:
        return []


def _reciprocal_rank_fusion(semantic_results: list, fts_results: list, k: int = 60) -> dict:
    """Combine semantic and FTS rankings using reciprocal rank fusion.

    Returns {pub_id: combined_score}.
    """
    scores = {}

    for rank, (idx, sim) in enumerate(semantic_results):
        pub_id = _pub_ids[idx]
        scores[pub_id] = scores.get(pub_id, 0) + 1.0 / (k + rank + 1)

    for rank, (pub_id, fts_rank) in enumerate(fts_results):
        scores[pub_id] = scores.get(pub_id, 0) + 1.0 / (k + rank + 1)

    return scores


def search(query: str, top_k: int = 20, year_min: int = None, year_max: int = None) -> list:
    """Search with hybrid scoring, query expansion, and cross-encoder re-ranking.

    Returns list of (pub_id, similarity_score) sorted by relevance.
    """
    load()

    # Step 1: Query expansion
    expanded_query = expand_query(query)

    # Step 2: Semantic search on abstracts (get more candidates for re-ranking)
    n_candidates = top_k * 5
    query_emb = _encode([expanded_query])
    semantic_results = _semantic_search(query_emb, n_candidates)

    # Step 2b: Semantic search on full-text chunks
    chunk_scores = _chunk_search(query_emb, n_candidates=200)

    # Step 3: Full-text search
    fts_results = _fts_search(query, limit=50)

    # Step 4: Reciprocal rank fusion (now with 3 sources)
    rrf_scores = _reciprocal_rank_fusion(semantic_results, fts_results)

    # Merge chunk results into RRF scores
    # Sort chunk results by score to create a ranking
    chunk_ranked = sorted(chunk_scores.items(), key=lambda x: -x[1])
    for rank, (pub_id, sim) in enumerate(chunk_ranked):
        rrf_scores[pub_id] = rrf_scores.get(pub_id, 0) + 1.0 / (60 + rank + 1)

    # Get original semantic scores for display
    semantic_scores = {}
    for idx, sim in semantic_results:
        pub_id = _pub_ids[idx]
        semantic_scores[pub_id] = sim

    # Sort by RRF score, filter, and dedup
    sorted_candidates = sorted(rrf_scores.items(), key=lambda x: -x[1])

    seen_titles = {}
    filtered = []
    for pub_id, rrf_score in sorted_candidates:
        if len(filtered) >= top_k * 3:  # Get enough for re-ranking
            break

        meta = _pub_meta.get(pub_id)
        if not meta:
            continue
        if _is_review_artifact(pub_id):
            continue
        if year_min and meta["year"] and meta["year"] < year_min:
            continue
        if year_max and meta["year"] and meta["year"] > year_max:
            continue

        title_key = meta["title_lower"]
        if title_key in seen_titles:
            prev_id = seen_titles[title_key]
            prev_meta = _pub_meta.get(prev_id, {})
            if meta["cited_by_count"] > prev_meta.get("cited_by_count", 0):
                filtered = [(pid, s, t, a) for pid, s, t, a in filtered if pid != prev_id]
                seen_titles[title_key] = pub_id
                filtered.append((pub_id, semantic_scores.get(pub_id, 0),
                                meta["title"], meta["abstract"]))
            continue

        seen_titles[title_key] = pub_id
        filtered.append((pub_id, semantic_scores.get(pub_id, 0),
                         meta["title"], meta["abstract"]))

    # Step 5: Cross-encoder re-ranking
    if _reranker_session and len(filtered) > 1:
        results = _rerank(expanded_query, filtered, top_n=top_k)
    else:
        results = [(c[0], c[1]) for c in filtered[:top_k]]

    return results
