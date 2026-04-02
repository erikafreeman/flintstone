"""Semantic search for Scopefish.

Uses the same ONNX embedding approach as Flintstone's search.py.
For now, wraps the keyword search in models.py.
When SPECTER2 model is available, will add embedding-based search.
"""

import os
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MODEL_DIR = os.path.join(DATA_DIR, "model_specter2")

_session = None
_tokenizer = None
_embeddings = None
_paper_ids = None


def _mean_pooling(token_embeddings, attention_mask):
    mask_expanded = np.expand_dims(attention_mask, axis=-1).astype(np.float32)
    sum_embeddings = np.sum(token_embeddings * mask_expanded, axis=1)
    sum_mask = np.clip(np.sum(mask_expanded, axis=1), a_min=1e-9, a_max=None)
    return sum_embeddings / sum_mask


def _normalize(embeddings):
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return embeddings / norms


def load():
    """Load ONNX model and embeddings if available."""
    global _session, _tokenizer, _embeddings, _paper_ids

    onnx_path = os.path.join(MODEL_DIR, "onnx", "model.onnx")
    tok_path = os.path.join(MODEL_DIR, "tokenizer.json")
    emb_path = os.path.join(DATA_DIR, "embeddings", "papers.npy")
    ids_path = os.path.join(DATA_DIR, "embeddings", "paper_ids.npy")

    if not os.path.exists(onnx_path):
        print("SPECTER2 model not found — semantic search disabled")
        return

    try:
        import onnxruntime as ort
        from tokenizers import Tokenizer

        _session = ort.InferenceSession(onnx_path)
        _tokenizer = Tokenizer.from_file(tok_path)

        if os.path.exists(emb_path):
            _embeddings = np.load(emb_path)
            _paper_ids = np.load(ids_path, allow_pickle=True)
            norms = np.linalg.norm(_embeddings, axis=1, keepdims=True)
            norms[norms == 0] = 1
            _embeddings = _embeddings / norms
            print(f"Loaded {len(_paper_ids)} paper embeddings for semantic search")
        else:
            print("No paper embeddings found — run scripts/build_embeddings.py")
    except ImportError:
        print("onnxruntime/tokenizers not installed — semantic search disabled")


def encode(texts: list, max_length: int = 384) -> np.ndarray:
    """Encode texts using the SPECTER2 model."""
    if _session is None or _tokenizer is None:
        return None

    encodings = [_tokenizer.encode(text) for text in texts]
    max_len = min(max(len(e.ids) for e in encodings), max_length)

    input_ids, attention_mask = [], []
    for enc in encodings:
        ids = enc.ids[:max_len]
        mask = enc.attention_mask[:max_len]
        pad_len = max_len - len(ids)
        input_ids.append(ids + [0] * pad_len)
        attention_mask.append(mask + [0] * pad_len)

    input_ids = np.array(input_ids, dtype=np.int64)
    attention_mask_arr = np.array(attention_mask, dtype=np.int64)

    outputs = _session.run(None, {
        "input_ids": input_ids,
        "attention_mask": attention_mask_arr,
    })
    embs = _mean_pooling(outputs[0], attention_mask_arr)
    return _normalize(embs)


def semantic_search(query: str, top_k: int = 30) -> list:
    """Search papers by semantic similarity.

    Returns list of (paper_id, similarity_score).
    """
    if _embeddings is None or _session is None:
        return []

    query_emb = encode([query])
    if query_emb is None:
        return []

    similarities = np.dot(_embeddings, query_emb.T).flatten()
    top_indices = np.argsort(similarities)[::-1][:top_k]

    return [
        (str(_paper_ids[idx]), float(similarities[idx]))
        for idx in top_indices
    ]
