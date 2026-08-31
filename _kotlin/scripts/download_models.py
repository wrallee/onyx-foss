#!/usr/bin/env python3
"""Download the pinned model artifacts recorded in MODELS.md."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "models"

SPECS = {
    "granite": {
        "repo": "ibm-granite/granite-embedding-311m-multilingual-r2",
        "revision": "44399559930365213510b1ee2eb15ded83374f0e",
        "directory": "granite-embedding-311m-multilingual-r2-int8-openvino",
        "files": {
            "README.md": None,
            "config.json": None,
            "config_sentence_transformers.json": None,
            "modules.json": None,
            "sentence_bert_config.json": None,
            "special_tokens_map.json": None,
            "tokenizer_config.json": None,
            "tokenizer.json": "0087c868b33bad550a78a08d19798cfd7f713cde4f020803b8f51f405503e15f",
            "1_Pooling/config.json": "781299da695e58439d70d491840da22ea0935d1d57d9646eb9725f1f19754e89",
            "openvino/openvino_model_qint8_quantized.xml": "9dc67b11a2f629359329ccef6c8a572477ee316375dcbf664fad1b8c90d812f3",
            "openvino/openvino_model_qint8_quantized.bin": "19cd99b8657e8f86529ce05384194b1bf3dbde94fd48a571a832344c119c27bc",
        },
    },
    "gte": {
        "repo": "Alibaba-NLP/gte-multilingual-reranker-base",
        "revision": "8215cf04918ba6f7b6a62bb44238ce2953d8831c",
        "directory": "gte-multilingual-reranker-base",
        "files": {
            "README.md": None,
            "config.json": None,
            "special_tokens_map.json": None,
            "tokenizer_config.json": None,
            "tokenizer.json": "d6f76fe13d42f80dcee0cb86a1aeb5f14f8909bb8a8782f7a4a4ad76697ef164",
            "model.safetensors": "10ebaa49322dd7e01a13a91c49810939e3f91f231aceaa47fdf0cab3083954f6",
        },
    },
    "bge": {
        "repo": "BAAI/bge-reranker-v2-m3",
        "revision": "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e",
        "directory": "bge-reranker-v2-m3",
        "files": {
            "README.md": None,
            "config.json": None,
            "special_tokens_map.json": None,
            "tokenizer_config.json": None,
            "sentencepiece.bpe.model": "cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865",
            "tokenizer.json": "69564b696052886ed0ac63fa393e928384e0f8caada38c1f4864a9bfbf379c15",
            "model.safetensors": "d9e3e081faff1eefb84019509b2f5558fd74c1a05a2c7db22f74174fcedb5286",
        },
    },
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(repo: str, revision: str, remote: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.parent.chmod(0o755)
    partial = target.with_suffix(target.suffix + ".partial")
    url = (
        f"https://huggingface.co/{repo}/resolve/{revision}/"
        f"{urllib.parse.quote(remote, safe='/')}?download=true"
    )
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "onyx-foss-kotlin-model-fetch/1.0"},
    )
    print(f"Downloading {repo}/{remote}", flush=True)
    with urllib.request.urlopen(request) as response, partial.open("wb") as output:
        total = int(response.headers.get("Content-Length") or 0)
        completed = 0
        report_at = 128 * 1024 * 1024
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
            completed += len(chunk)
            if completed >= report_at:
                total_text = f"/{total // (1024 * 1024)}" if total else ""
                print(f"  {completed // (1024 * 1024)}{total_text} MiB", flush=True)
                report_at += 128 * 1024 * 1024
    os.replace(partial, target)
    target.chmod(0o644)


def install(name: str) -> None:
    spec = SPECS[name]
    destination = MODELS / spec["directory"]
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "REVISION").write_text(spec["revision"] + "\n", encoding="utf-8")

    for remote, expected in spec["files"].items():
        target = destination / remote
        if target.is_file() and (expected is None or sha256(target) == expected):
            print(f"Using verified {target.relative_to(ROOT)}")
            continue
        download(spec["repo"], spec["revision"], remote, target)
        if expected is not None:
            actual = sha256(target)
            if actual != expected:
                target.unlink(missing_ok=True)
                raise RuntimeError(
                    f"SHA-256 mismatch for {remote}: expected {expected}, got {actual}"
                )
            print(f"Verified {remote}")


GTE_REMOTE_CODE = {
    "repo": "Alibaba-NLP/new-impl",
    "revision": "40ced75c3017eb27626c9d4ea981bde21a2662f4",
    "files": {
        "configuration.py": "3411088045ffb8a9a0aa9936eae275896b39983a2ee5b08f091b44e6289e4fe4",
        "modeling.py": "374670b416fcc82f081c9cd28b5fd61c2bd91bbe18eb4798fcc48a81f9c250a0",
    },
}


def install_gte_remote_code() -> None:
    destination = MODELS / SPECS["gte"]["directory"] / "remote_code"
    destination.mkdir(parents=True, exist_ok=True)
    for remote, expected in GTE_REMOTE_CODE["files"].items():
        target = destination / remote
        if target.is_file() and sha256(target) == expected:
            print(f"Using verified {target.relative_to(ROOT)}")
            continue
        download(
            GTE_REMOTE_CODE["repo"],
            GTE_REMOTE_CODE["revision"],
            remote,
            target,
        )
        actual = sha256(target)
        if actual != expected:
            target.unlink(missing_ok=True)
            raise RuntimeError(
                f"SHA-256 mismatch for {remote}: expected {expected}, got {actual}"
            )
        print(f"Verified {remote}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--with-rerankers",
        action="store_true",
        help="Also download the GTE and BGE reranker candidates (about 2.8 GiB).",
    )
    args = parser.parse_args()

    install("granite")
    if args.with_rerankers:
        install("gte")
        install_gte_remote_code()
        install("bge")
    print("Model download complete.")


if __name__ == "__main__":
    main()
