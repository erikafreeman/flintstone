"""Download ONNX models from HuggingFace Hub at startup.

Models are too large for git (616MB) so we download them on first run.
This runs automatically in the Dockerfile before the app starts.
"""

import os
import shutil
from huggingface_hub import hf_hub_download, snapshot_download

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

MODELS = {
    "model_specter2": {
        "repo": "sentence-transformers/all-mpnet-base-v2",
        "files": {
            "onnx/model.onnx": "onnx/model.onnx",
            "tokenizer.json": "tokenizer.json",
        },
    },
    "model_reranker": {
        "repo": "cross-encoder/ms-marco-MiniLM-L-6-v2",
        "files": {
            "onnx/model.onnx": "onnx/model.onnx",
            "tokenizer.json": "tokenizer.json",
        },
    },
    "model": {
        "repo": "sentence-transformers/all-MiniLM-L6-v2",
        "files": {
            "onnx/model.onnx": "onnx/model.onnx",
            "tokenizer.json": "tokenizer.json",
        },
    },
}


def download_all():
    for model_name, config in MODELS.items():
        model_dir = os.path.join(DATA_DIR, model_name)
        onnx_path = os.path.join(model_dir, "onnx", "model.onnx")

        if os.path.exists(onnx_path):
            size_mb = os.path.getsize(onnx_path) / 1e6
            print(f"  {model_name}: already present ({size_mb:.0f} MB)")
            continue

        print(f"  Downloading {model_name} from {config['repo']}...")
        os.makedirs(os.path.join(model_dir, "onnx"), exist_ok=True)

        for remote_path, local_path in config["files"].items():
            dest = os.path.join(model_dir, local_path)
            os.makedirs(os.path.dirname(dest), exist_ok=True)

            try:
                downloaded = hf_hub_download(
                    repo_id=config["repo"],
                    filename=remote_path,
                    local_dir=model_dir,
                )
                # hf_hub_download may put files in a cache dir, so copy if needed
                if os.path.abspath(downloaded) != os.path.abspath(dest):
                    shutil.copy2(downloaded, dest)
                print(f"    ✓ {local_path}")
            except Exception as e:
                print(f"    ✗ {local_path}: {e}")

        if os.path.exists(onnx_path):
            size_mb = os.path.getsize(onnx_path) / 1e6
            print(f"  {model_name}: ready ({size_mb:.0f} MB)")
        else:
            print(f"  {model_name}: FAILED — model file not found")


if __name__ == "__main__":
    print("Downloading ONNX models...")
    download_all()
    print("Done.")
