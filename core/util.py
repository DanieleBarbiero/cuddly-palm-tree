# core/util.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def now_iso() -> str:
    # ISO-ish, stable for filenames/logs
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=encoding)
    os.replace(tmp, path)


def atomic_write_json(path: Path, obj: Any) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2) + "\n")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def jsonl_append(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    line = json.dumps(obj, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def safe_ext(path: Path) -> str:
    return path.suffix.lower().lstrip(".")


def item_type_from_path(path: Path) -> str:
    ext = safe_ext(path)
    return ext if ext else "unknown"


def short_id(s: str, n: int = 12) -> str:
    return s[:n]