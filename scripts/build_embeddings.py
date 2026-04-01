"""Generate embeddings for all publications using ONNX runtime + tokenizers (no PyTorch needed)."""

import os
import sys
import sqlite3
import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flinstone.db")
EMBEDDINGS_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "embeddings.npy")
PUB_IDS_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "pub_ids.npy")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "model")
ONNX_PATH = os.path.join(MODEL_DIR, "onnx", "model.onnx")
TOKENIZER_PATH = os.path.join(MODEL_DIR, "tokenizer.json")


def mean_pooling(token_embeddings, attention_mask):
    """Mean pooling — take average of all token embeddings, weighted by attention mask."""
    mask_expanded = np.expand_dims(attention_mask, axis=-1).astype(np.float32)
    sum_embeddings = np.sum(token_embeddings * mask_expanded, axis=1)
    sum_mask = np.clip(np.sum(mask_expanded, axis=1), a_min=1e-9, a_max=None)
    return sum_embeddings / sum_mask


def normalize(embeddings):
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return embeddings / norms


def encode_batch(session, tokenizer, texts, max_length=256):
    """Encode a batch of texts into embeddings."""
    encodings = [tokenizer.encode(text) for text in texts]

    # Pad to same length
    max_len = min(max(len(e.ids) for e in encodings), max_length)

    input_ids = []
    attention_mask = []
    token_type_ids = []

    for enc in encodings:
        ids = enc.ids[:max_len]
        mask = enc.attention_mask[:max_len]
        ttype = enc.type_ids[:max_len]

        # Pad
        pad_len = max_len - len(ids)
        ids = ids + [0] * pad_len
        mask = mask + [0] * pad_len
        ttype = ttype + [0] * pad_len

        input_ids.append(ids)
        attention_mask.append(mask)
        token_type_ids.append(ttype)

    input_ids = np.array(input_ids, dtype=np.int64)
    attention_mask_arr = np.array(attention_mask, dtype=np.int64)
    token_type_ids = np.array(token_type_ids, dtype=np.int64)

    outputs = session.run(
        None,
        {
            "input_ids": input_ids,
            "attention_mask": attention_mask_arr,
            "token_type_ids": token_type_ids,
        },
    )

    token_embeddings = outputs[0]  # (batch, seq_len, hidden_dim)
    sentence_embeddings = mean_pooling(token_embeddings, attention_mask_arr)
    sentence_embeddings = normalize(sentence_embeddings)

    return sentence_embeddings


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("Loading publications with abstracts...")
    cur.execute(
        "SELECT id, title, abstract FROM publications WHERE abstract != '' AND abstract IS NOT NULL"
    )
    rows = cur.fetchall()
    conn.close()

    print(f"Found {len(rows)} publications with abstracts")

    pub_ids = [row[0] for row in rows]
    texts = [f"{row[1] or ''} {row[2] or ''}" for row in rows]

    print(f"Loading ONNX model from {ONNX_PATH}...")
    session = ort.InferenceSession(ONNX_PATH)
    tokenizer = Tokenizer.from_file(TOKENIZER_PATH)

    print("Generating embeddings...")
    batch_size = 32
    all_embeddings = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        embs = encode_batch(session, tokenizer, batch)
        all_embeddings.append(embs)

        done = min(i + batch_size, len(texts))
        if done % 500 == 0 or done == len(texts):
            print(f"  {done}/{len(texts)} texts embedded")

    embeddings = np.vstack(all_embeddings).astype(np.float32)
    pub_ids_arr = np.array(pub_ids)

    np.save(EMBEDDINGS_PATH, embeddings)
    np.save(PUB_IDS_PATH, pub_ids_arr)

    print(f"\nSaved embeddings: {embeddings.shape} to {EMBEDDINGS_PATH}")
    print(f"Saved pub IDs: {pub_ids_arr.shape} to {PUB_IDS_PATH}")


if __name__ == "__main__":
    main()
